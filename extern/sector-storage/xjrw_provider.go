package sectorstorage

import (
	"context"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-storage/storage"
	"golang.org/x/xerrors"
	"io"
	"os"
	"time"
)

func (m *Manager) AddPiece(ctx context.Context, sector abi.SectorID, existingPieces []abi.UnpaddedPieceSize, sz abi.UnpaddedPieceSize, r io.Reader) (abi.PieceInfo, error) {
	log.Infof("xjrw AddPiece begin %v sz = %v", sector, sz)
	t1 := time.Now()
	defer func() {
		t2 := time.Now()
		log.Infof("xjrw cast mgr AddPiece %v, %v, %v, %v", sector, t2.Sub(t1), t1, t2)
	}()

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

	var out abi.PieceInfo
	err = m.sched.Schedule(ctx, sector, sealtasks.TTAddPiece, selector, schedNop, func(ctx context.Context, w Worker) error {
		inf, e := w.Info(ctx)
		if e != nil {
			return e
		}

		startSector(stores.SectorName(sector), inf.Hostname, sealtasks.TTAddPiece)
		defer endSector(stores.SectorName(sector), inf.Hostname, sealtasks.TTAddPiece)

		p, err := w.AddPiece(ctx, sector, existingPieces, sz, r)
		if err != nil {
			return err
		}
		out = p
		return nil
	})

	return out, err
}

func (m *Manager) SealPreCommit1(ctx context.Context, sector abi.SectorID, ticket abi.SealRandomness, pieces []abi.PieceInfo) (out storage.PreCommit1Out, err error) {
	log.Info("xjrw SealPreCommit1 begin ", sector)
	t1 := time.Now()
	defer func() {
		t2 := time.Now()
		log.Infof("xjrw cast mgr SealPreCommit1 %v, %v, %v, %v", sector, t2.Sub(t1), t1, t2)
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector, stores.FTUnsealed, stores.FTSealed|stores.FTCache); err != nil {
		return nil, xerrors.Errorf("acquiring sector lock: %w", err)
	}

	// TODO: also consider where the unsealed data sits

	selector := newAllocSelector(m.index, sector, stores.FTCache|stores.FTSealed, stores.PathSealing)

	err = m.sched.Schedule(ctx, sector, sealtasks.TTPreCommit1, selector, schedNop, func(ctx context.Context, w Worker) error {
		inf, e := w.Info(ctx)
		if e != nil {
			return e
		}

		startSector(stores.SectorName(sector), inf.Hostname, sealtasks.TTPreCommit1)
		defer endSector(stores.SectorName(sector), inf.Hostname, sealtasks.TTPreCommit1)

		p, err := w.SealPreCommit1(ctx, sector, ticket, pieces)
		if err != nil {
			return err
		}

		out = p
		return nil
	})

	return out, err
}

func (m *Manager) SealPreCommit2(ctx context.Context, sector abi.SectorID, phase1Out storage.PreCommit1Out) (out storage.SectorCids, err error) {
	log.Info("xjrw SealPreCommit2 begin ", sector)
	t1 := time.Now()
	defer func() {
		t2 := time.Now()
		log.Infof("xjrw cast mgr SealPreCommit2 %v, %v, %v, %v", sector, t2.Sub(t1), t1, t2)
	}()

	_, exist := m.mapReal[sector]
	if os.Getenv("LOTUS_PLDEGE") != "" && !exist {
		go ShellExecute(os.Getenv("LOTUS_PLDEGE"))
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector, stores.FTSealed, stores.FTCache); err != nil {
		return storage.SectorCids{}, xerrors.Errorf("acquiring sector lock: %w", err)
	}

	selector := newExistingSelector(m.index, sector, stores.FTCache|stores.FTSealed, false)

	err = m.sched.Schedule(ctx, sector, sealtasks.TTPreCommit2, selector, schedFetch(sector, stores.FTCache|stores.FTSealed, stores.PathSealing, stores.AcquireMove), func(ctx context.Context, w Worker) error {
		inf, e := w.Info(ctx)
		if e != nil {
			return e
		}

		startSector(stores.SectorName(sector), inf.Hostname, sealtasks.TTPreCommit2)
		defer endSector(stores.SectorName(sector), inf.Hostname, sealtasks.TTPreCommit2)

		p, err := w.SealPreCommit2(ctx, sector, phase1Out)
		if err != nil {
			return err
		}
		out = p
		return nil
	})
	return out, err
}

func (m *Manager) SealCommit1(ctx context.Context, sector abi.SectorID, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, pieces []abi.PieceInfo, cids storage.SectorCids) (out storage.Commit1Out, err error) {
	log.Info("xjrw SealCommit1 begin ", sector)
	t1 := time.Now()
	defer func() {
		t2 := time.Now()
		log.Infof("xjrw cast mgr SealCommit1 %v, %v, %v, %v", sector, t2.Sub(t1), t1, t2)
	}()

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
		defer endSector(stores.SectorName(sector), inf.Hostname, sealtasks.TTCommit1)

		p, err := w.SealCommit1(ctx, sector, ticket, seed, pieces, cids)
		if err != nil {
			return err
		}
		out = p
		return nil
	})
	return out, err
}

func (m *Manager) SealCommit2(ctx context.Context, sector abi.SectorID, phase1Out storage.Commit1Out) (out storage.Proof, err error) {
	log.Info("xjrw SealCommit2 begin ", sector)
	t1 := time.Now()
	defer func() {
		t2 := time.Now()
		log.Infof("xjrw cast mgr SealCommit2 %v, %v, %v, %v", sector, t2.Sub(t1), t1, t2)
	}()

	selector := newTaskSelector()

	err = m.sched.Schedule(ctx, sector, sealtasks.TTCommit2, selector, schedNop, func(ctx context.Context, w Worker) error {
		inf, e := w.Info(ctx)
		if e != nil {
			return e
		}

		startSector(stores.SectorName(sector), inf.Hostname, sealtasks.TTCommit2)
		defer endSector(stores.SectorName(sector), inf.Hostname, sealtasks.TTCommit2)

		p, err := w.SealCommit2(ctx, sector, phase1Out)
		if err != nil {
			return err
		}
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
