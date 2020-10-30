package sectorstorage

import (
	"context"
	"encoding/json"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/specs-storage/storage"
	"golang.org/x/xerrors"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"time"
)

func (m *Manager) AddPiece(ctx context.Context, sector abi.SectorID, existingPieces []abi.UnpaddedPieceSize, sz abi.UnpaddedPieceSize, r io.Reader) (abi.PieceInfo, error) {
	log.Infof("xjrw AddPiece begin %v sz = %v", sector, sz)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector, stores.FTNone, stores.FTUnsealed); err != nil {
		return abi.PieceInfo{}, xerrors.Errorf("acquiring sector lock: %w", err)
	}

	if len(existingPieces) != 0 {
		m.mapReal[sector] = struct{}{}
	}

	var selector WorkerSelector
	var err error
	if len(existingPieces) == 0 { // new
		selector = newAllocSelector(m.index, sector, stores.FTUnsealed, stores.PathSealing)
	} else { // use existing
		selector = newExistingSelector(m.index, sector, stores.FTUnsealed, false)
	}

	if timeStr := os.Getenv("ADDPIECE_DELAY_TIME"); timeStr != "" {
		timeNow := time.Now().Unix()
		if timedelay, err := strconv.Atoi(timeStr); err == nil {
			if m.addPieceStartTime != 0 && timeNow-m.addPieceStartTime < int64(timedelay) {
				m.lk.Lock()
				m.addPieceStartTime += int64(timedelay)
				m.lk.Unlock()
				log.Info("%v addPiece  delay ", sector, m.addPieceStartTime-timeNow)
				time.Sleep(time.Second * time.Duration(m.addPieceStartTime-timeNow))
			} else {
				m.addPieceStartTime = timeNow
			}
		}
	}

	var out abi.PieceInfo
	err = m.sched.Schedule(ctx, sector, sealtasks.TTAddPiece, selector, schedNop, func(ctx context.Context, w Worker) error {
		inf, e := w.Info(ctx)
		if e != nil {
			return e
		}

		startSector(stores.SectorName(sector), inf.Hostname, sealtasks.TTAddPiece)

		t1 := time.Now()
		defer func() {
			t2 := time.Now()
			log.Infof("xjrw cast mgr AddPiece %v, %v, %v, %v", sector, t2.Sub(t1), t1, t2)
		}()

		p, err := w.AddPiece(ctx, sector, existingPieces, sz, r)
		if err != nil {
			return err
		}

		endSector(stores.SectorName(sector), inf.Hostname, sealtasks.TTAddPiece)
		out = p
		return nil
	})

	return out, err
}

func (m *Manager) SealPreCommit1(ctx context.Context, sector abi.SectorID, ticket abi.SealRandomness, pieces []abi.PieceInfo) (out storage.PreCommit1Out, err error) {
	log.Info("xjrw SealPreCommit1 begin ", sector)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector, stores.FTUnsealed, stores.FTSealed|stores.FTCache); err != nil {
		return nil, xerrors.Errorf("acquiring sector lock: %w", err)
	}

	// TODO: also consider where the unsealed data sits

	selector := newAllocSelector(m.index, sector, stores.FTCache|stores.FTSealed, stores.PathSealing)

	err = m.sched.Schedule(ctx, sector, sealtasks.TTPreCommit1, selector, schedFetch(sector, stores.FTUnsealed, stores.PathSealing, stores.AcquireMove), func(ctx context.Context, w Worker) error {
		inf, e := w.Info(ctx)
		if e != nil {
			return e
		}

		startSector(stores.SectorName(sector), inf.Hostname, sealtasks.TTPreCommit1)

		t1 := time.Now()
		defer func() {
			t2 := time.Now()
			log.Infof("xjrw cast mgr SealPreCommit1 %v, %v, %v, %v", sector, t2.Sub(t1), t1, t2)
		}()

		p, err := w.SealPreCommit1(ctx, sector, ticket, pieces)
		if err != nil {
			return err
		}

		m.mapChan[sector] = make(chan struct{})
		endSector(stores.SectorName(sector), inf.Hostname, sealtasks.TTPreCommit1)
		out = p
		return nil
	})

	return out, err
}

func (m *Manager) SealPreCommit2(ctx context.Context, sector abi.SectorID, phase1Out storage.PreCommit1Out) (out storage.SectorCids, err error) {
	log.Info("xjrw SealPreCommit2 begin ", sector)

	m.lkChan.Lock()
	ch, ok := m.mapChan[sector]
	m.lkChan.Unlock()
	if ok {
		log.Info("xjrw start wait for %v", sector)
		select {
		case res := <-ch:
			log.Info("xjrw get channel %v %v", sector, res)
		case <-time.After(time.Minute * 30):
			m.lkChan.Lock()
			delete(m.mapChan, sector)
			log.Info("xjrw timeout %v %v", sector, m.mapChan)
			m.lkChan.Unlock()
		}
	} else {
		log.Info("possible already get channle %v", sector)
	}

	host := findSector(stores.SectorName(sector), sealtasks.TTPreCommit2)
	if host == "" {
		log.Errorf("not find p2host: %v", sector)
		host = m.SelectWorkerPreComit2(sector)
		if host == "" {
			log.Errorf("p2 not online: %v", sector)
			return storage.SectorCids{}, xerrors.Errorf("p2 not online: %v", sector)
		}
	}
	m.addTask(host, sector)
	defer m.removeTask(host, sector)
	m.setWorker(host, sector)

	_, exist := m.mapReal[sector]
	if os.Getenv("LOTUS_PLDEGE") != "" && !exist {
		if findP2Start(stores.SectorName(sector), sealtasks.TTPreCommit2) == "" && m.getP2Worker() {
			log.Infof("ShellExecute %v", sector)
			go ShellExecute(os.Getenv("LOTUS_PLDEGE"))
		} else {
			log.Infof("repeated SealPreCommit2 %v", sector)
		}
	}

	saveP2Start(stores.SectorName(sector), sealtasks.TTPreCommit2)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector, stores.FTSealed, stores.FTCache); err != nil {
		return storage.SectorCids{}, xerrors.Errorf("acquiring sector lock: %w", err)
	}

	selector := newExistingSelector(m.index, sector, stores.FTCache|stores.FTSealed, true)

	err = m.sched.Schedule(ctx, sector, sealtasks.TTPreCommit2, selector, schedFetch(sector, stores.FTCache|stores.FTSealed, stores.PathSealing, stores.AcquireMove), func(ctx context.Context, w Worker) error {
		inf, e := w.Info(ctx)
		if e != nil {
			return e
		}

		startSector(stores.SectorName(sector), inf.Hostname, sealtasks.TTPreCommit2)
		log.Infof("startworker %v %v", inf.Hostname, sector)

		t1 := time.Now()
		defer func() {
			t2 := time.Now()
			log.Infof("xjrw cast mgr SealPreCommit2 %v, %v, %v, %v", sector, t2.Sub(t1), t1, t2)
		}()

		defer m.UnselectWorkerPreComit2(inf.Hostname, sector)

		p, err := w.SealPreCommit2(ctx, sector, phase1Out)
		if err != nil {
			return err
		}

		endSector(stores.SectorName(sector), inf.Hostname, sealtasks.TTPreCommit2)
		out = p
		return nil
	})
	return out, err
}

func (m *Manager) SealCommit1(ctx context.Context, sector abi.SectorID, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, pieces []abi.PieceInfo, cids storage.SectorCids) (out storage.Commit1Out, err error) {
	log.Info("xjrw SealCommit1 begin ", sector)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector, stores.FTSealed, stores.FTCache); err != nil {
		return storage.Commit1Out{}, xerrors.Errorf("acquiring sector lock: %w", err)
	}

	// NOTE: We set allowFetch to false in so that we always execute on a worker
	// with direct access to the data. We want to do that because this step is
	// generally very cheap / fast, and transferring data is not worth the effort
	selector := newExistingSelector(m.index, sector, stores.FTCache|stores.FTSealed, false)

	err = m.sched.Schedule(ctx, sector, sealtasks.TTCommit1, selector, schedFetch(sector, stores.FTCache|stores.FTSealed, stores.PathSealing, stores.AcquireMove), func(ctx context.Context, w Worker) error {
		inf, e := w.Info(ctx)
		if e != nil {
			return e
		}

		startSector(stores.SectorName(sector), inf.Hostname, sealtasks.TTCommit1)

		t1 := time.Now()
		defer func() {
			t2 := time.Now()
			log.Infof("xjrw cast mgr SealCommit1 %v, %v, %v, %v", sector, t2.Sub(t1), t1, t2)
		}()

		p, err := w.SealCommit1(ctx, sector, ticket, seed, pieces, cids)
		if err != nil {
			return err
		}

		endSector(stores.SectorName(sector), inf.Hostname, sealtasks.TTCommit1)
		out = p
		return nil
	})
	return out, err
}

func (m *Manager) SealCommit2(ctx context.Context, sector abi.SectorID, phase1Out storage.Commit1Out) (out storage.Proof, err error) {
	log.Info("xjrw SealCommit2 begin ", sector)

	selector := newTaskSelector()

	err = m.sched.Schedule(ctx, sector, sealtasks.TTCommit2, selector, schedNop, func(ctx context.Context, w Worker) error {
		inf, e := w.Info(ctx)
		if e != nil {
			return e
		}

		startSector(stores.SectorName(sector), inf.Hostname, sealtasks.TTCommit2)

		t1 := time.Now()
		defer func() {
			t2 := time.Now()
			log.Infof("xjrw cast mgr SealCommit2 %v, %v, %v, %v", sector, t2.Sub(t1), t1, t2)
		}()

		p, err := w.SealCommit2(ctx, sector, phase1Out)
		if err != nil {
			return err
		}

		endSector(stores.SectorName(sector), inf.Hostname, sealtasks.TTCommit2)
		out = p
		return nil
	})

	return out, err
}

func (m *Manager) FinalizeSector(ctx context.Context, sector abi.SectorID, keepUnsealed []storage.Range) error {
	log.Info("xjrw FinalizeSector begin ", sector)
	t1 := time.Now()
	defer func() {
		t2 := time.Now()
		log.Infof("xjrw cast mgr FinalizeSector %v, %v, %v, %v", sector, t2.Sub(t1), t1, t2)
	}()

	return m.oldFinalizeSector(ctx, sector, keepUnsealed)
}

func (m *Manager) addTask(host string, sector abi.SectorID) {
	m.lkTask.Lock()
	defer m.lkTask.Unlock()

	_, ok := m.mapP2Tasks[host]
	if !ok {
		m.mapP2Tasks[host] = map[abi.SectorID]struct{}{}
	}

	m.mapP2Tasks[host][sector] = struct{}{}
	log.Infof("addTask %v %v %v", sector, host, m.mapP2Tasks[host])
}

func (m *Manager) removeTask(host string, sector abi.SectorID) {
	m.lkTask.Lock()
	defer m.lkTask.Unlock()

	mapSector, ok := m.mapP2Tasks[host]
	if !ok {
		log.Infof("removeTask no exit %v %v", sector, host)
		return
	}

	delete(mapSector, sector)
	log.Infof("removeTask %v %v %v", sector, host, mapSector)
}

func (m *Manager) getTask(host string) map[abi.SectorID]struct{} {
	m.lkTask.Lock()
	defer m.lkTask.Unlock()

	mapSector, ok := m.mapP2Tasks[host]
	if !ok {
		log.Infof("%v getTask %v %v", host, mapSector, ok)
		return map[abi.SectorID]struct{}{}
	}
	log.Infof("%v getTask %v", host, mapSector)

	return mapSector
}

func (m *Manager) UnselectWorkerPreComit2(host string, sector abi.SectorID) {
	m.sched.workersLk.Lock()
	defer m.sched.workersLk.Unlock()

	id := -1
	for wid, worker := range m.sched.workers {
		if worker.info.Hostname == host {
			id = int(wid)
			break
		}
	}
	if id != -1 {
		delete(m.sched.workers[WorkerID(id)].p2Tasks, sector)
		log.Infof("UnselectWorkerPreComit2 wid = %v host = %v sector = %v p2Tasks = %v", id, host, sector, m.sched.workers[WorkerID(id)].p2Tasks)
	} else {
		log.Errorf("UnselectWorkerPreComit2 not find %v %v", host, sector)
	}
}

func (m *Manager) SelectWorkerPreComit2(sector abi.SectorID) string {
	m.sched.workersLk.Lock()
	defer m.sched.workersLk.Unlock()

	tasks := make(map[WorkerID]int)
	for wid, worker := range m.sched.workers {
		if _, supported := worker.taskTypes[sealtasks.TTPreCommit2]; !supported {
			continue
		}
		tasks[wid] = len(worker.p2Tasks)
		log.Infof("SelectWorkerPreComit2 wid = %v host = %v p2Size = %v p2Tasks = %v", wid, worker.info.Hostname, len(worker.p2Tasks), worker.p2Tasks)
	}

	host := ""
	minNum := 100
	var w WorkerID
	for wid, num := range tasks {
		if num < minNum {
			host = m.sched.workers[wid].info.Hostname
			w = wid
			minNum = num
		}
	}

	if host != "" {
		m.sched.workers[WorkerID(w)].p2Tasks[sector] = struct{}{}
		saveP2Worker(stores.SectorName(sector), host, sealtasks.TTPreCommit2)
		log.Infof("saveP2Worker %v %v", sector, host)
	} else {
		log.Infof("saveP2Worker not find %v", sector)
	}

	return host
}

func (m *Manager) handlerP1(w http.ResponseWriter, r *http.Request) {
	log.Info("method = ", r.Method)
	log.Info("URL = ", r.URL)
	log.Info("header = ", r.Header)
	log.Info("body = ", r.Body)
	log.Info(r.RemoteAddr, "connect success")

	body, _ := ioutil.ReadAll(r.Body)
	data := make(map[string]string)
	err := json.Unmarshal(body, &data)
	if err != nil {
		log.Error("Unmarshal error %+v", err)
		return
	}

	sector, err := stores.ParseSectorID(data["sector"])
	if err != nil {
		log.Error("data error %+v", err)
		return
	}

	m.lkChan.Lock()
	if ch, ok := m.mapChan[sector]; ok {
		close(ch)
		delete(m.mapChan, sector)
		log.Infof("delete channel %v %v", sector, m.mapChan)
	} else {
		log.Error("handlerP1 not find ", sector)
	}
	m.lkChan.Unlock()

	w.Write([]byte("ok"))
}

func (m *Manager) handlerP2(w http.ResponseWriter, r *http.Request) {
	log.Info("method = ", r.Method)
	log.Info("URL = ", r.URL)
	log.Info("header = ", r.Header)
	log.Info("body = ", r.Body)
	log.Info(r.RemoteAddr, "connect success")

	body, _ := ioutil.ReadAll(r.Body)
	data := make(map[string]string)
	err := json.Unmarshal(body, &data)
	if err != nil {
		log.Error("Unmarshal error %+v", err)
		return
	}

	sector, err := stores.ParseSectorID(data["sector"])
	if err != nil {
		log.Error("data error %+v", err)
		return
	}

	log.Info("sector = ", sector)
	host := m.SelectWorkerPreComit2(sector)
	log.Info("return host = ", host)

	w.Write([]byte(host))
}

func (m *Manager) setWorker(host string, sector abi.SectorID) {
	m.sched.workersLk.Lock()
	defer m.sched.workersLk.Unlock()

	id := -1
	for wid, handle := range m.sched.workers {
		if handle.info.Hostname == host {
			m.sched.workers[wid].p2Tasks[sector] = struct{}{}
			id = int(wid)
		}
	}

	if id != -1 {
		log.Infof("p2 online %v %v %v", id, sector, host)
	} else {
		log.Infof("p2 not online %v %v", sector, host)
	}
}

func (m *Manager) getP2Worker() bool {
	m.sched.workersLk.Lock()
	defer m.sched.workersLk.Unlock()

	for _, worker := range m.sched.workers {
		if _, supported := worker.taskTypes[sealtasks.TTPreCommit2]; supported {
			return true
		}
	}

	log.Errorf("have no p2 worker")
	return false
}
