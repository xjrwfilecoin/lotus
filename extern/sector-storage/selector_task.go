package sectorstorage

import (
	"context"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/actors/abi"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
)

type taskSelector struct {
	best   []stores.StorageInfo //nolint: unused, structcheck
	sector abi.SectorID
}

func newTaskSelector(sector abi.SectorID) *taskSelector {
	return &taskSelector{
		sector: sector,
	}
}

func (s *taskSelector) Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, whnd *workerHandle) (bool, error) {
	tasks, err := whnd.w.TaskTypes(ctx)
	if err != nil {
		return false, xerrors.Errorf("getting supported worker task types: %w", err)
	}
	_, supported := tasks[task]

	inf, err := whnd.w.Info(ctx)
	if err != nil {
		return false, xerrors.Errorf("getting worker info: %w", err)
	}

	if _, exist := groupState[inf.Hostname]; exist {
		pwk := findSector(stores.SectorName(s.sector), sealtasks.TTAddPiece)
		log.Infof("xjrw %v task = %s  pwk = %s hostname = %s", s.sector, task, pwk, inf.Hostname)
		if pwk == "" {
			return false, xerrors.Errorf("%v not exist", s.sector)
		}
		if groupState[pwk].GroupName != groupState[inf.Hostname].GroupName {
			log.Infof("%v not in group %v  %v  %v  %v", s.sector, groupState[pwk].GroupName, pwk, inf.Hostname, groupState[inf.Hostname].GroupName)
			return false, nil
		}
	}

	return supported, nil
}

func (s *taskSelector) Cmp(ctx context.Context, _ sealtasks.TaskType, a, b *workerHandle) (bool, error) {
	atasks, err := a.w.TaskTypes(ctx)
	if err != nil {
		return false, xerrors.Errorf("getting supported worker task types: %w", err)
	}
	btasks, err := b.w.TaskTypes(ctx)
	if err != nil {
		return false, xerrors.Errorf("getting supported worker task types: %w", err)
	}
	if len(atasks) != len(btasks) {
		return len(atasks) < len(btasks), nil // prefer workers which can do less
	}

	return a.active.utilization(a.info.Resources) < b.active.utilization(b.info.Resources), nil
}

var _ WorkerSelector = &allocSelector{}
