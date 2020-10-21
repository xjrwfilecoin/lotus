package sectorstorage

import (
	"context"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/specs-storage/storage"
	"golang.org/x/xerrors"
	"io"
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

		endSector(stores.SectorName(sector), inf.Hostname, sealtasks.TTPreCommit1)
		out = p
		return nil
	})

	return out, err
}

func (m *Manager) SealPreCommit2(ctx context.Context, sector abi.SectorID, phase1Out storage.PreCommit1Out) (out storage.SectorCids, err error) {
	log.Info("xjrw SealPreCommit2 begin ", sector)

	_, exist := m.mapReal[sector]
	if os.Getenv("LOTUS_PLDEGE") != "" && !exist {
		if findP2Start(stores.SectorName(sector), sealtasks.TTPreCommit2) == "" {
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

		t1 := time.Now()
		defer func() {
			t2 := time.Now()
			log.Infof("xjrw cast mgr SealPreCommit2 %v, %v, %v, %v", sector, t2.Sub(t1), t1, t2)
		}()

		defer m.DeleteWorkerPreComit2(m.sched.findWorker(inf.Hostname), sector)

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

func (m *Manager) DeleteWorkerPreComit2(wid int64, sector abi.SectorID) {
	if wid != -1 {
		delete(m.sched.workers[WorkerID(wid)].p2Tasks, sector)
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
	}

	for wid, jobs := range m.WorkerJobs() {
		for _, job := range jobs {
			if job.Task == sealtasks.TTPreCommit2 {
				_, exist := m.sched.workers[WorkerID(wid)].p2Tasks[job.Sector]
				if !exist {
					tasks[WorkerID(wid)]++
				} else {

				}
			}
		}
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
	}

	return host
}

func (sh *scheduler) findWorker(host string) int64 {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()

	for wid, worker := range sh.workers {
		if worker.info.Hostname == host {
			return int64(wid)
		}
	}

	return -1
}
