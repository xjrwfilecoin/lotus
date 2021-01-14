package sectorstorage

import (
	"context"
	"encoding/json"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"github.com/filecoin-project/specs-storage/storage"
	"golang.org/x/xerrors"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"time"
)

func (m *Manager) AddPiece(ctx context.Context, sector storage.SectorRef, existingPieces []abi.UnpaddedPieceSize, sz abi.UnpaddedPieceSize, r io.Reader) (abi.PieceInfo, error) {
	log.Infof("xjrw AddPiece begin %v sz = %v", sector, sz)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector.ID, storiface.FTNone, storiface.FTUnsealed); err != nil {
		return abi.PieceInfo{}, xerrors.Errorf("acquiring sector lock: %w", err)
	}

	if len(existingPieces) != 0 {
		m.mapReal[sector.ID] = struct{}{}
	}

	var selector WorkerSelector
	var err error
	if len(existingPieces) == 0 { // new
		selector = newAllocSelector(m.index, sector.ID, storiface.FTUnsealed, storiface.PathSealing)
	} else { // use existing
		selector = newExistingSelector(m.index, sector.ID, storiface.FTUnsealed, false)
	}

	if timeStr := os.Getenv("ADDPIECE_DELAY_TIME"); timeStr != "" {
		timeNow := time.Now().Unix()
		if timedelay, err := strconv.Atoi(timeStr); err == nil {
			if m.addPieceStartTime != 0 && timeNow-m.addPieceStartTime < int64(timedelay) {
				m.lk.Lock()
				m.addPieceStartTime += int64(timedelay)
				m.lk.Unlock()
				log.Infof("%v addPiece  delay %v", sector, m.addPieceStartTime-timeNow)
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

		startSector(storiface.SectorName(sector.ID), inf.Hostname, sealtasks.TTAddPiece)

		t1 := time.Now()
		log.Infof("start AddPiece : %v", sector)
		defer func() {
			t2 := time.Now()
			log.Infof("xjrw cast mgr AddPiece %v, %v, %v, %v", sector, t2.Sub(t1), t1, t2)
		}()

		p, err := m.waitSimpleCall(ctx)(w.AddPiece(ctx, sector, existingPieces, sz, r))
		if err != nil {
			return err
		}

		endSector(storiface.SectorName(sector.ID), inf.Hostname, sealtasks.TTAddPiece)

		if p != nil {
			out = p.(abi.PieceInfo)
		}
		return nil
	})

	return out, err
}

func (m *Manager) SealPreCommit1(ctx context.Context, sector storage.SectorRef, ticket abi.SealRandomness, pieces []abi.PieceInfo) (out storage.PreCommit1Out, err error) {
	log.Info("xjrw SealPreCommit1 begin ", sector)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wk, wait, cancel, err := m.getWork(ctx, sealtasks.TTPreCommit1, sector, ticket, pieces)
	if err != nil {
		return nil, xerrors.Errorf("getWork: %w", err)
	}
	defer cancel()

	var waitErr error
	waitRes := func() {
		p, werr := m.waitWork(ctx, wk)
		if werr != nil {
			waitErr = werr
			return
		}
		if p != nil {
			out = p.(storage.PreCommit1Out)
		}
	}

	if wait { // already in progress
		waitRes()
		return out, waitErr
	}

	if err := m.index.StorageLock(ctx, sector.ID, storiface.FTUnsealed, storiface.FTSealed|storiface.FTCache); err != nil {
		return nil, xerrors.Errorf("acquiring sector lock: %w", err)
	}

	// TODO: also consider where the unsealed data sits

	selector := newAllocSelector(m.index, sector.ID, storiface.FTCache|storiface.FTSealed, storiface.PathSealing)

	err = m.sched.Schedule(ctx, sector, sealtasks.TTPreCommit1, selector, m.schedFetch(sector, storiface.FTUnsealed, storiface.PathSealing, storiface.AcquireMove), func(ctx context.Context, w Worker) error {
		inf, e := w.Info(ctx)
		if e != nil {
			return e
		}

		startSector(storiface.SectorName(sector.ID), inf.Hostname, sealtasks.TTPreCommit1)

		t1 := time.Now()
		log.Infof("start SealPreCommit1 : %v", sector)
		defer func() {
			t2 := time.Now()
			log.Infof("xjrw cast mgr SealPreCommit1 %v, %v, %v, %v", sector, t2.Sub(t1), t1, t2)
		}()

		err := m.startWork(ctx, w, wk)(w.SealPreCommit1(ctx, sector, ticket, pieces))
		if err != nil {
			return err
		}

		waitRes()

		endSector(storiface.SectorName(sector.ID), inf.Hostname, sealtasks.TTPreCommit1)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return out, waitErr
}

func (m *Manager) SealPreCommit2(ctx context.Context, sector storage.SectorRef, phase1Out storage.PreCommit1Out) (out storage.SectorCids, err error) {
	log.Info("xjrw SealPreCommit2 begin ", sector, p1p2State)

	//m.lkChan.Lock()
	//ch, ok := m.mapChan[sector]
	//m.lkChan.Unlock()
	//if ok {
	//	log.Infof("xjrw start wait for %v", sector)
	//	select {
	//	case res := <-ch:
	//		log.Infof("xjrw get channel %v %v", sector, res)
	//	case <-time.After(time.Minute * 30):
	//		m.lkChan.Lock()
	//		delete(m.mapChan, sector)
	//		log.Infof("xjrw timeout %v %v", sector, m.mapChan)
	//		m.lkChan.Unlock()
	//	}
	//} else {
	//	log.Infof("possible already get channle %v %v", sector, m.mapChan)
	//}

	host := findSector(storiface.SectorName(sector.ID), sealtasks.TTPreCommit2)
	if host == "" && p1p2State == 0 {
		log.Errorf("not find p2host: %v", sector)
		//host = m.SelectWorkerPreComit2(sector.ID)
		if host == "" {
			log.Errorf("p2 not online: %v", sector)
			return storage.SectorCids{}, xerrors.Errorf("p2 not online: %v", sector)
		}
	}
	if p1p2State != 0 {
		pwk := findSector(storiface.SectorName(sector.ID), sealtasks.TTPreCommit1)
		if pwk == "" {
			log.Errorf("p1 not find: %v", sector)
			return storage.SectorCids{}, xerrors.Errorf("p1 not find: %v", sector)
		}
		saveP2Worker(storiface.SectorName(sector.ID), pwk, sealtasks.TTPreCommit2)
	}
	m.addTask(host, sector.ID)
	defer m.removeTask(host, sector.ID)
	m.setWorker(host, sector.ID)
	defer m.UnselectWorkerPreComit2(host, sector.ID)

	_, exist := m.mapReal[sector.ID]
	if os.Getenv("LOTUS_PLDEGE") != "" && !exist {
		if findP2Start(storiface.SectorName(sector.ID), sealtasks.TTPreCommit2) == "" {
			pledgeTime := 0
			if str := os.Getenv("AUTO_PLEDGE_TIME"); str != "" {
				if interval, err := strconv.Atoi(str); err == nil {
					pledgeTime = interval
				}
			}
			if pledgeTime < 0 {
				log.Infof("SealPreCommit2 ShellExecute %v", sector)
				m.pledgeTask()
			}
		} else {
			log.Infof("repeated PreCommit2 %v", sector)
		}
	}

	saveP2Start(storiface.SectorName(sector.ID), sealtasks.TTPreCommit2)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wk, wait, cancel, err := m.getWork(ctx, sealtasks.TTPreCommit2, sector, phase1Out)
	if err != nil {
		return storage.SectorCids{}, xerrors.Errorf("getWork: %w", err)
	}
	defer cancel()

	var waitErr error
	waitRes := func() {
		p, werr := m.waitWork(ctx, wk)
		if werr != nil {
			waitErr = werr
			return
		}
		if p != nil {
			out = p.(storage.SectorCids)
		}
	}

	if wait { // already in progress
		waitRes()
		return out, waitErr
	}

	if err := m.index.StorageLock(ctx, sector.ID, storiface.FTSealed, storiface.FTCache); err != nil {
		return storage.SectorCids{}, xerrors.Errorf("acquiring sector lock: %w", err)
	}

	selector := newExistingSelector(m.index, sector.ID, storiface.FTCache|storiface.FTSealed, true)
	err = m.sched.Schedule(ctx, sector, sealtasks.TTPreCommit2, selector, m.schedFetch(sector, storiface.FTCache|storiface.FTSealed, storiface.PathSealing, storiface.AcquireMove), func(ctx context.Context, w Worker) error {
		inf, e := w.Info(ctx)
		if e != nil {
			return e
		}

		startSector(storiface.SectorName(sector.ID), inf.Hostname, sealtasks.TTPreCommit2)
		log.Infof("startworker %v %v", inf.Hostname, sector.ID)

		t1 := time.Now()
		log.Infof("start SealPreCommit2 : %v", sector.ID)
		defer func() {
			t2 := time.Now()
			log.Infof("xjrw cast mgr SealPreCommit2 %v, %v, %v, %v", sector, t2.Sub(t1), t1, t2)
		}()

		err := m.startWork(ctx, w, wk)(w.SealPreCommit2(ctx, sector, phase1Out))
		if err != nil {
			return err
		}

		waitRes()

		endSector(storiface.SectorName(sector.ID), inf.Hostname, sealtasks.TTPreCommit2)

		return nil
	})
	if err != nil {
		return storage.SectorCids{}, err
	}

	return out, waitErr
}

func (m *Manager) SealCommit1(ctx context.Context, sector storage.SectorRef, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, pieces []abi.PieceInfo, cids storage.SectorCids) (out storage.Commit1Out, err error) {
	log.Info("xjrw SealCommit1 begin ", sector)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wk, wait, cancel, err := m.getWork(ctx, sealtasks.TTCommit1, sector, ticket, seed, pieces, cids)
	if err != nil {
		return storage.Commit1Out{}, xerrors.Errorf("getWork: %w", err)
	}
	defer cancel()

	var waitErr error
	waitRes := func() {
		p, werr := m.waitWork(ctx, wk)
		if werr != nil {
			waitErr = werr
			return
		}
		if p != nil {
			out = p.(storage.Commit1Out)
		}
	}

	if wait { // already in progress
		waitRes()
		return out, waitErr
	}

	if err := m.index.StorageLock(ctx, sector.ID, storiface.FTSealed, storiface.FTCache); err != nil {
		return storage.Commit1Out{}, xerrors.Errorf("acquiring sector lock: %w", err)
	}

	// NOTE: We set allowFetch to false in so that we always execute on a worker
	// with direct access to the data. We want to do that because this step is
	// generally very cheap / fast, and transferring data is not worth the effort
	selector := newExistingSelector(m.index, sector.ID, storiface.FTCache|storiface.FTSealed, false)

	err = m.sched.Schedule(ctx, sector, sealtasks.TTCommit1, selector, m.schedFetch(sector, storiface.FTCache|storiface.FTSealed, storiface.PathSealing, storiface.AcquireMove), func(ctx context.Context, w Worker) error {
		inf, e := w.Info(ctx)
		if e != nil {
			return e
		}

		startSector(storiface.SectorName(sector.ID), inf.Hostname, sealtasks.TTCommit1)

		t1 := time.Now()
		log.Infof("start SealCommit1 : %v", sector)
		defer func() {
			t2 := time.Now()
			log.Infof("xjrw cast mgr SealCommit1 %v, %v, %v, %v", sector, t2.Sub(t1), t1, t2)
		}()

		err := m.startWork(ctx, w, wk)(w.SealCommit1(ctx, sector, ticket, seed, pieces, cids))
		if err != nil {
			return err
		}

		waitRes()

		endSector(storiface.SectorName(sector.ID), inf.Hostname, sealtasks.TTCommit1)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return out, waitErr
}

func (m *Manager) SealCommit2(ctx context.Context, sector storage.SectorRef, phase1Out storage.Commit1Out) (out storage.Proof, err error) {
	log.Info("xjrw SealCommit2 begin ", sector)

	wk, wait, cancel, err := m.getWork(ctx, sealtasks.TTCommit2, sector, phase1Out)
	if err != nil {
		return storage.Proof{}, xerrors.Errorf("getWork: %w", err)
	}
	defer cancel()

	var waitErr error
	waitRes := func() {
		p, werr := m.waitWork(ctx, wk)
		if werr != nil {
			waitErr = werr
			return
		}
		if p != nil {
			out = p.(storage.Proof)
		}
	}

	if wait { // already in progress
		waitRes()
		return out, waitErr
	}

	selector := newTaskSelector()

	err = m.sched.Schedule(ctx, sector, sealtasks.TTCommit2, selector, schedNop, func(ctx context.Context, w Worker) error {
		inf, e := w.Info(ctx)
		if e != nil {
			return e
		}

		startSector(storiface.SectorName(sector.ID), inf.Hostname, sealtasks.TTCommit2)

		t1 := time.Now()
		log.Infof("start SealCommit2 : %v", sector)
		defer func() {
			t2 := time.Now()
			log.Infof("xjrw cast mgr SealCommit2 %v, %v, %v, %v", sector, t2.Sub(t1), t1, t2)
		}()

		err := m.startWork(ctx, w, wk)(w.SealCommit2(ctx, sector, phase1Out))
		if err != nil {
			return err
		}

		waitRes()

		endSector(storiface.SectorName(sector.ID), inf.Hostname, sealtasks.TTCommit2)

		return nil
	})

	if err != nil {
		return nil, err
	}

	return out, waitErr
}

func (m *Manager) FinalizeSector(ctx context.Context, sector storage.SectorRef, keepUnsealed []storage.Range) error {
	log.Info("xjrw FinalizeSector begin ", sector)
	t1 := time.Now()
	defer func() {
		t2 := time.Now()
		log.Infof("xjrw cast mgr FinalizeSector %v, %v, %v, %v", sector, t2.Sub(t1), t1, t2)
	}()

	return m.oldFinalizeSector(ctx, sector, keepUnsealed)
}

func (m *Manager) addTask(host string, sector abi.SectorID) {
	if p1p2State != 0 {
		return
	}
	m.lkTask.Lock()
	defer m.lkTask.Unlock()

	_, ok := m.mapP2Tasks[host]
	if !ok {
		m.mapP2Tasks[host] = map[abi.SectorID]struct{}{}
	}

	m.mapP2Tasks[host][sector] = struct{}{}
	//log.Infof("addTask %v %v %v", sector, host, m.mapP2Tasks[host])
}

func (m *Manager) removeTask(host string, sector abi.SectorID) {
	if p1p2State != 0 {
		return
	}
	m.lkTask.Lock()
	defer m.lkTask.Unlock()

	mapSector, ok := m.mapP2Tasks[host]
	if !ok {
		log.Infof("removeTask no exit %v %v", sector, host)
		return
	}

	delete(mapSector, sector)
	//log.Infof("removeTask %v %v %v", sector, host, mapSector)
}

func (m *Manager) getTask(host string) map[abi.SectorID]struct{} {
	if p1p2State != 0 {
		return map[abi.SectorID]struct{}{}
	}
	m.lkTask.Lock()
	defer m.lkTask.Unlock()

	mapSector, ok := m.mapP2Tasks[host]
	if !ok {
		log.Infof("%v getTask %v %v", host, mapSector, ok)
		return map[abi.SectorID]struct{}{}
	}
	//log.Infof("%v getTask %v", host, mapSector)

	return mapSector
}

func (m *Manager) UnselectWorkerPreComit2(host string, sector abi.SectorID) {
	if p1p2State != 0 {
		return
	}
	m.sched.workersLk.Lock()
	defer m.sched.workersLk.Unlock()

	var id WorkerID
	find := false
	for wid, worker := range m.sched.workers {
		if worker.info.Hostname == host {
			id = wid
			find = true
			break
		}
	}
	if find {
		delete(m.sched.workers[id].p2Tasks, sector)
		log.Infof("UnselectWorkerPreComit2 wid = %v host = %v sector = %v p2Size = %v", id, host, sector, len(m.sched.workers[WorkerID(id)].p2Tasks))
	} else {
		log.Errorf("UnselectWorkerPreComit2 not find %v %v", host, sector)
	}
}

func (m *Manager) SelectWorkerPreComit2(sector abi.SectorID) string {
	if p1p2State != 0 {
		return ""
	}

	m.sched.workersLk.Lock()
	defer m.sched.workersLk.Unlock()

	tasks := make(map[WorkerID]int)
	for wid, worker := range m.sched.workers {
		if _, supported := worker.taskTypes[sealtasks.TTPreCommit2]; !supported {
			continue
		}

		if worker.enabled == false {
			continue
		}

		if _, exit := worker.p2Tasks[sector]; exit {
			log.Infof("%v SelectWorkerPreComit2 delete %v", worker.info.Hostname, sector)
			delete(worker.p2Tasks, sector)
		}

		if P2NumberLimit > 0 && len(worker.p2Tasks) >= P2NumberLimit {
			log.Infof("%v P2 exceed %v %v", worker.info.Hostname, len(worker.p2Tasks), P2NumberLimit)
			continue
		}

		var avai int64
		log.Infof("storeIDs %v", worker.storeIDs)
		for id, _ := range worker.storeIDs {
			si, err := m.index.StorageFsi(stores.ID(id))
			if err == nil {
				avai = avai + si.Available
			}
		}

		avai = avai / 1024 / 1024 / 1024
		log.Infof("%v P2 space %vG %vG", worker.info.Hostname, avai, p2SpaceLimit)
		if avai < p2SpaceLimit {
			log.Infof("%v P2 no space %vG %vG", worker.info.Hostname, avai, p2SpaceLimit)
			continue
		}

		tasks[wid] = len(worker.p2Tasks)
		log.Infof("SelectWorkerPreComit2 wid = %v host = %v p2Size = %v", wid, worker.info.Hostname, len(worker.p2Tasks))
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
		saveP2Worker(storiface.SectorName(sector), host, sealtasks.TTPreCommit2)
		log.Infof("saveP2Worker %v %v", sector, host)
	} else {
		saveP2Worker(storiface.SectorName(sector), "", sealtasks.TTPreCommit2)
		log.Infof("saveP2Worker not find %v", sector)
	}

	return host
}

func (m *Manager) handlerP1(w http.ResponseWriter, r *http.Request) {
	log.Info("method = ", r.Method)
	log.Info("URL = ", r.URL)
	log.Info("header = ", r.Header)
	log.Info("body = ", r.Body)
	log.Info(r.RemoteAddr, " connect success")

	body, _ := ioutil.ReadAll(r.Body)
	data := make(map[string]string)
	err := json.Unmarshal(body, &data)
	if err != nil {
		log.Error("Unmarshal error %+v", err)
		w.Write([]byte(err.Error()))
		return
	}

	log.Info("result :", data["result"])

	sector, err := storiface.ParseSectorID(data["sector"])
	if err != nil {
		log.Error("data error %+v", err)
		w.Write([]byte(err.Error()))
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

	w.Write([]byte(""))
}

func (m *Manager) handlerP2(w http.ResponseWriter, r *http.Request) {
	log.Info("method = ", r.Method)
	log.Info("URL = ", r.URL)
	log.Info("header = ", r.Header)
	log.Info("body = ", r.Body)
	log.Info(r.RemoteAddr, " connect success")

	body, _ := ioutil.ReadAll(r.Body)
	data := make(map[string]string)
	err := json.Unmarshal(body, &data)
	if err != nil {
		log.Error("Unmarshal error %+v", err)
		w.Write([]byte(""))
		return
	}

	sector, err := storiface.ParseSectorID(data["sector"])
	if err != nil {
		log.Error("data error %+v", err)
		w.Write([]byte(""))
		return
	}

	log.Info("sector = ", sector)
	host := m.SelectWorkerPreComit2(sector)
	log.Info("return host = ", host)

	w.Write([]byte(host))
}

func (m *Manager) setWorker(host string, sector abi.SectorID) {
	if p1p2State != 0 {
		return
	}
	m.sched.workersLk.Lock()
	defer m.sched.workersLk.Unlock()

	var id WorkerID
	find := false
	for wid, handle := range m.sched.workers {
		if handle.info.Hostname == host {
			m.sched.workers[wid].p2Tasks[sector] = struct{}{}
			id = wid
			find = true
		}
	}

	if find {
		log.Infof("p2 online %v %v %v", id, sector, host)
	} else {
		log.Infof("p2 not online %v %v", sector, host)
	}
}

func (m *Manager) getP2Worker() bool {
	if p1p2State != 0 {
		return true
	}
	m.sched.workersLk.Lock()
	defer m.sched.workersLk.Unlock()

	for _, worker := range m.sched.workers {
		if _, supported := worker.taskTypes[sealtasks.TTPreCommit2]; supported && worker.enabled {
			if P2NumberLimit > 0 && len(worker.p2Tasks) >= P2NumberLimit {
				log.Infof("%v P2 exceed %v %v", worker.info.Hostname, len(worker.p2Tasks), P2NumberLimit)
				continue
			} else {
				var avai int64
				log.Infof("storeIDs %v", worker.storeIDs)
				for id, _ := range worker.storeIDs {
					si, err := m.index.StorageFsi(stores.ID(id))
					if err == nil {
						avai = avai + si.Available
					}
				}

				avai = avai / 1024 / 1024 / 1024
				log.Infof("%v P2 space %vG %vG", worker.info.Hostname, avai, p2SpaceLimit)
				if avai < p2SpaceLimit {
					log.Infof("%v P2 no space %vG %vG", worker.info.Hostname, avai, p2SpaceLimit)
				} else {
					return true
				}
			}
		}
	}

	log.Errorf("have no p2 worker")
	return false
}

func (m *Manager) SetSectorState(ctx context.Context, sector abi.SectorNumber, state string) {
	if p1p2State != 0 {
		return
	}
	if state != "Removed" && state != "FailedUnrecoverable" && state != "Removing" {
		return
	}

	log.Infof("SetSectorState %v %v", sector, state)
	m.sched.workersLk.Lock()
	for id, handle := range m.sched.workers {
		for s, _ := range handle.p2Tasks {
			if s.Number == sector {
				log.Infof("SetSectorState delete %v %v", sector, state)
				delete(m.sched.workers[id].p2Tasks, s)
			}
		}
	}
	m.sched.workersLk.Unlock()

	m.lkTask.Lock()
	for host, mp := range m.mapP2Tasks {
		for s, _ := range mp {
			if s.Number == sector {
				log.Infof("SetSectorState remove %v %v", sector, state)
				delete(m.mapP2Tasks[host], s)
			}
		}
	}
	m.lkTask.Unlock()
}

func (m *Manager) autoAddTask(ctx context.Context) {
	delayTime := 180
	if str := os.Getenv("AUTO_PLEDGE_TIME"); str != "" {
		if delay, err := strconv.Atoi(str); err == nil {
			delayTime = delay
		}
	}
	intervalTime := 5
	if str := os.Getenv("AUTO_INTERVAL_TIME"); str != "" {
		if interval, err := strconv.Atoi(str); err == nil {
			intervalTime = interval
		}
	}
	if delayTime < 0 || intervalTime < 0 {
		log.Infof("cancel autoAddTask %v %v", delayTime, intervalTime)
		return
	}
	time.Sleep(time.Duration(delayTime) * time.Minute)
	ticker := time.NewTicker(time.Duration(intervalTime) * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.pledgeTask()
		case <-ctx.Done():
			return
		}
	}
}

func (m *Manager) pledgeTask() {
	p1Server := 0
	tasks := 0

	m.sched.workersLk.Lock()
	for _, worker := range m.sched.workers {
		if _, supported := worker.taskTypes[sealtasks.TTPreCommit1]; supported && worker.enabled {
			p1Server++
			tasks += len(worker.addPieceRuning) + len(worker.p1Running)
		}
	}
	m.sched.workersLk.Unlock()

	totalTasks := p1Server * p1Limit
	if m.getP2Worker() && tasks < totalTasks {
		go ShellExecute(os.Getenv("LOTUS_PLDEGE"))
		log.Infof("autoAddTask success %v %v", tasks, totalTasks)
	} else {
		log.Infof("autoAddTask failed %v %v", tasks, totalTasks)
	}
}
