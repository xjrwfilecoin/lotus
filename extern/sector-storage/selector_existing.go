package sectorstorage

import (
	"context"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
)

type existingSelector struct {
	index      stores.SectorIndex
	sector     abi.SectorID
	alloc      stores.SectorFileType
	allowFetch bool
}

func newExistingSelector(index stores.SectorIndex, sector abi.SectorID, alloc stores.SectorFileType, allowFetch bool) *existingSelector {
	return &existingSelector{
		index:      index,
		sector:     sector,
		alloc:      alloc,
		allowFetch: allowFetch,
	}
}

func (s *existingSelector) Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, whnd *workerHandle) (bool, error) {
	tasks, err := whnd.w.TaskTypes(ctx)
	if err != nil {
		return false, xerrors.Errorf("getting supported worker task types: %w", err)
	}
	if _, supported := tasks[task]; !supported {
		return false, nil
	}

	inf, err := whnd.w.Info(ctx)
	if err != nil {
		return false, xerrors.Errorf("getting worker info: %w", err)
	}

	if task == sealtasks.TTPreCommit2 {
		pwk := findSector(stores.SectorName(s.sector), sealtasks.TTPreCommit2)
		log.Infof("xjrw %v task = %s  pwk = %s hostname = %s", s.sector, task, pwk, inf.Hostname)

		if pwk == "" {
			return false, xerrors.Errorf("%v not exist", s.sector)
		}

		if pwk != inf.Hostname {
			log.Infof("%v P1&P2 not in same server %v  %v", s.sector, pwk, inf.Hostname)
			return false, nil
		}
	}

	//if group, exist := groupState[inf.Hostname]; task == sealtasks.TTPreCommit2 {
	//	pwk1 := findSector(stores.SectorName(s.sector), sealtasks.TTPreCommit1)
	//	//log.Infof("xjrw %v task = %s  pwk = %s hostname = %s", s.sector, task, pwk1, inf.Hostname)
	//	if pwk1 == "" {
	//		return false, xerrors.Errorf("%v not exist", s.sector)
	//	}
	//
	//	if exist {
	//		if groupState[pwk1].GroupName != group.GroupName {
	//			//log.Infof("%v not in group %v  %v  %v  %v", s.sector, groupState[pwk1].GroupName, pwk1, inf.Hostname, group.GroupName)
	//			return false, nil
	//		}
	//		pwk2 := findSector(stores.SectorName(s.sector), sealtasks.TTPreCommit2)
	//		if pwk2 != "" {
	//			if pwk2 != inf.Hostname {
	//				//log.Infof("%v P1&P2 not in same server %v  %v", s.sector, pwk1, inf.Hostname)
	//				return false, nil
	//			}
	//		} else {
	//			if index := int(s.sector.Number) % getGroupCount(group.GroupName); index != group.GroupIndex {
	//				//log.Infof("%v index %v P1&P2 is different %v  %v", s.sector, index, inf.Hostname, group)
	//				return false, nil
	//			}
	//		}
	//	} else {
	//		if pwk1 != inf.Hostname {
	//			//log.Infof("%v P1&P2 not in same server %v  %v", s.sector, pwk1, inf.Hostname)
	//			return false, nil
	//		}
	//	}
	//}
	paths, err := whnd.w.Paths(ctx)
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

	best, err := s.index.StorageFindSector(ctx, s.sector, s.alloc, ssize, s.allowFetch)
	if err != nil {
		return false, xerrors.Errorf("finding best storage: %w", err)
	}

	for _, info := range best {
		if _, ok := have[info.ID]; ok {
			return true, nil
		}
	}

	return false, nil
}

func (s *existingSelector) Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) {
	return a.utilization() < b.utilization(), nil
}

var _ WorkerSelector = &existingSelector{}
