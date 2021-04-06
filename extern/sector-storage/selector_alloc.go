package sectorstorage

import (
	"context"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

type allocSelector struct {
	index  stores.SectorIndex
	sector abi.SectorID
	alloc  storiface.SectorFileType
	ptype  storiface.PathType
}

func newAllocSelector(index stores.SectorIndex, sector abi.SectorID, alloc storiface.SectorFileType, ptype storiface.PathType) *allocSelector {
	return &allocSelector{
		index:  index,
		sector: sector,
		alloc:  alloc,
		ptype:  ptype,
	}
}

func (s *allocSelector) Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, whnd *workerHandle) (bool, error) {
	tasks, err := whnd.workerRpc.TaskTypes(ctx)
	if err != nil {
		return false, xerrors.Errorf("getting supported worker task types: %w", err)
	}
	if _, supported := tasks[task]; !supported {
		return false, nil
	}

	inf, err := whnd.workerRpc.Info(ctx)
	if err != nil {
		return false, xerrors.Errorf("getting worker info: %w", err)
	}

	if task == sealtasks.TTPreCommit1 {
		pwk := findSector(storiface.SectorName(s.sector), sealtasks.TTAddPiece)
		//log.Infof("xjrw %v task = %s  pwk = %s hostname = %s", s.sector, task, pwk, inf.Hostname)

		if pwk == "" {
			return false, xerrors.Errorf("%v not exist", s.sector)
		}

		if pwk != inf.Hostname {
			//log.Infof("%v AP&P1 not in same server %v  %v", s.sector, pwk, inf.Hostname)
			return false, nil
		}
	}
	paths, err := whnd.workerRpc.Paths(ctx)
	if err != nil {
		return false, xerrors.Errorf("getting worker paths: %w", err)
	}

	have := map[stores.ID]struct{}{}
	for _, path := range paths {
		have[path.ID] = struct{}{}
	}

	ssize, err := spt.SectorSize()
	if err != nil {
		return false, xerrors.Errorf("getting sector size: %w", err)
	}

	best, err := s.index.StorageBestAlloc(ctx, s.alloc, ssize, s.ptype)
	if err != nil {
		return false, xerrors.Errorf("finding best alloc storage: %w", err)
	}

	for _, info := range best {
		if _, ok := have[info.ID]; ok {
			return true, nil
		}
	}

	return false, nil
}

func (s *allocSelector) Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) {
	return a.utilization() < b.utilization(), nil
}

var _ WorkerSelector = &allocSelector{}
