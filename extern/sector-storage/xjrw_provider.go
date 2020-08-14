package sectorstorage

import (
	"context"
	"github.com/filecoin-project/go-fil-markets/filestore"
	"github.com/filecoin-project/go-padreader"
	"github.com/filecoin-project/sector-storage/sealtasks"
	"github.com/filecoin-project/sector-storage/stores"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-storage/storage"
	nr "github.com/filecoin-project/storage-fsm/lib/nullreader"
	"golang.org/x/xerrors"
	"io"
	"os"
	"time"
)

//import (
//	"context"
//	"github.com/filecoin-project/sector-storage/ffiwrapper"
//	"github.com/filecoin-project/sector-storage/sealtasks"
//	"github.com/filecoin-project/sector-storage/stores"
//	"github.com/filecoin-project/specs-actors/actors/abi"
//	"github.com/filecoin-project/specs-storage/storage"
//	storage2 "github.com/filecoin-project/specs-storage/storage"
//	nr "github.com/filecoin-project/storage-fsm/lib/nullreader"
//	"golang.org/x/xerrors"
//	"io"
//	"os"
//	"time"
//)

//type xjrwLocalWorkerPathProvider struct {
//	w    *LocalWorker
//	task sealtasks.TaskType
//}
//
//func (l *xjrwLocalWorkerPathProvider) AcquireSector(ctx context.Context, sector abi.SectorID, existing stores.SectorFileType, allocate stores.SectorFileType, sealing stores.PathType) (stores.SectorPaths, func(), error) {
//
//	rpath := stores.SectorPaths{}
//	done := func() {}
//
//	if l.task == sealtasks.TTPreCommit1 {
//		rpath = stores.GetSectorLocalPath(sector)
//	} else if l.task == sealtasks.TTPreCommit2 {
//		rpath = stores.GetSectorLocalPath(sector)
//	}
//
//	//if l.task == sealtasks.TTPreCommit1 || l.task == sealtasks.TTAddPiece {
//	//	rpath = stores.GetSectorBeegfsPath(sector)
//	//} else if l.task == sealtasks.TTPreCommit2 {
//	//	rpath = stores.GetSectorLocalPath(sector)
//	//	done = func() {
//	//		bgfs := stores.GetSectorBeegfsPath(sector)
//	//		err2 := os.RemoveAll(bgfs.Sealed)
//	//		err3 := os.RemoveAll(bgfs.Cache)
//	//		log.Infof("xjrw remove beegfs file : %v, %v", err2, err3)
//	//
//	//		ft := stores.FTSealed
//	//		err2 = l.w.sindex.StorageDeclareSector(ctx, l.w.localStore.MyID(), sector, ft, true)
//	//		log.Infof("xjrw %s <%+v> StorageDeclareSector %s : %v", l.task, ft.String(), sector, l.w.localStore.MyID(), err2)
//	//
//	//		ft = stores.FTCache
//	//		err2 = l.w.sindex.StorageDeclareSector(ctx, l.w.localStore.MyID(), sector, ft, true)
//	//		log.Infof("xjrw %s <%+v> StorageDeclareSector %s : %v", l.task, ft.String(), sector, l.w.localStore.MyID(), err2)
//	//	}
//	//} else if l.task == sealtasks.TTFinalize {
//	//	rpath = stores.GetSectorBeegfsFinalPath(sector)
//	//}
//
//	log.Infof("xjrw xjrwLocalWorkerPathProvider.AcquireSector %s return: %+v", l.task, rpath)
//	return rpath, done, nil
//}
//
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
////func (l *readonlyProvider) AcquireSector(ctx context.Context, id abi.SectorID, existing stores.SectorFileType, allocate stores.SectorFileType, sealing stores.PathType) (stores.SectorPaths, func(), error) {
////	// 做证明的时候调用，需要遍历目录找到sealed cache的文件位置
////	var ret stores.SectorPaths
////	ret.Id = id
////
////	dirstr := os.Getenv("PROV_ROOT")
////	if dirstr == "" {
////		panic("PROV_ROOT not set")
////	}
////
////	rd, err := ioutil.ReadDir(dirstr)
////	if err != nil {
////		panic(err)
////	}
////	for _, fi := range rd {
////		if fi.IsDir() {
////			sub := filepath.Join(dirstr, fi.Name())
////			if l.fileIsExist(sub, id) {
////				//log.Infof("xjrw find %v at %s", sector, sub)
////				ft := stores.FTUnsealed
////				ret.Unsealed = filepath.Join(sub, ft.String(), stores.SectorName(id))
////				ft = stores.FTCache
////				ret.Cache = filepath.Join(sub, ft.String(), stores.SectorName(id))
////				ft = stores.FTSealed
////				ret.Sealed = filepath.Join(sub, ft.String(), stores.SectorName(id))
////				break
////			}
////		}
////	}
////
////	log.Infof("xjrw readonlyProvider.AcquireSector return: %+v", ret)
////	return ret, func() {}, nil
////}
////
////func (l *readonlyProvider) fileIsExist(path string, sector abi.SectorID) bool {
////	target1 := filepath.Join(path, "cache", stores.SectorName(sector))
////	target2 := filepath.Join(path, "sealed", stores.SectorName(sector))
////	s1, err1 := os.Stat(target1)
////	s2, err2 := os.Stat(target2)
////	if err1 == nil && s1.IsDir() && err2 == nil && !s2.IsDir() {
////		return true
////	}
////	return false
////}
//
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
////func (m *Manager) CheckProvable(ctx context.Context, spt abi.RegisteredSealProof, sectors []abi.SectorID) ([]abi.SectorID, error) {
////	var bad []abi.SectorID
////
////	ssize, err := spt.SectorSize()
////	if err != nil {
////		return nil, err
////	}
////
////	// TODO: More better checks
////	for _, sector := range sectors {
////		err := func() error {
////			ctx, cancel := context.WithCancel(ctx)
////			defer cancel()
////
////			// locked, err := m.index.StorageTryLock(ctx, sector, stores.FTSealed|stores.FTCache, stores.FTNone)
////			// if err != nil {
////			// 	return xerrors.Errorf("acquiring sector lock: %w", err)
////			// }
////
////			// if !locked {
////			// 	log.Warnw("CheckProvable Sector FAULT: can't acquire read lock", "sector", sector, "sealed")
////			// 	bad = append(bad, sector)
////			// 	return nil
////			// }
////
////			lp, _, err := (&readonlyProvider{}).AcquireSector(ctx, sector, stores.FTSealed|stores.FTCache, stores.FTNone, false)
////			if err != nil {
////				return xerrors.Errorf("acquire sector in checkProvable: %w", err)
////			}
////
////			if lp.Sealed == "" || lp.Cache == "" {
////				log.Warnw("CheckProvable Sector FAULT: cache an/or sealed paths not found", "sector", sector, "sealed", lp.Sealed, "cache", lp.Cache)
////				bad = append(bad, sector)
////				return nil
////			}
////
////			toCheck := map[string]int64{
////				lp.Sealed:                        1,
////				filepath.Join(lp.Cache, "t_aux"): 0,
////				filepath.Join(lp.Cache, "p_aux"): 0,
////			}
////
////			addCachePathsForSectorSize(toCheck, lp.Cache, ssize)
////
////			for p, sz := range toCheck {
////				st, err := os.Stat(p)
////				if err != nil {
////					log.Warnw("CheckProvable Sector FAULT: sector file stat error", "sector", sector, "sealed", lp.Sealed, "cache", lp.Cache, "file", p, "err", err)
////					bad = append(bad, sector)
////					return nil
////				}
////
////				if sz != 0 {
////					if st.Size() != int64(ssize)*sz {
////						log.Warnw("CheckProvable Sector FAULT: sector file is wrong size", "sector", sector, "sealed", lp.Sealed, "cache", lp.Cache, "file", p, "size", st.Size(), "expectSize", int64(ssize)*sz)
////						bad = append(bad, sector)
////						return nil
////					}
////				}
////			}
////
////			return nil
////		}()
////		if err != nil {
////			return nil, err
////		}
////	}
////
////	return bad, nil
////}
//
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
////func (s *existingSelector) Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, whnd *workerHandle) (bool, error) {
////
////	// commit1 需要在worker上面执行
////	ignore := task == sealtasks.TTCommit1
////	tasks, err := whnd.w.TaskTypes(ctx)
////	if err != nil {
////		return false, xerrors.Errorf("getting supported worker task types: %w", err)
////	}
////	if _, supported := tasks[task]; !supported && !ignore {
////		return false, nil
////	}
////
////	paths, err := whnd.w.Paths(ctx)
////	if err != nil {
////		return false, xerrors.Errorf("getting worker paths: %w", err)
////	}
////
////	have := map[stores.ID]struct{}{}
////	for _, path := range paths {
////		have[path.ID] = struct{}{}
////	}
////
////	for _, info := range s.best {
////		if _, ok := have[info.ID]; ok {
////			return true, nil
////		}
////	}
////
////	return false, nil
////}
//
//type xjrwSelector struct {
//	index  stores.SectorIndex
//	alloc  stores.SectorFileType
//	ptask  sealtasks.TaskType
//	sector abi.SectorID
//}
//
//func newxjrwSelector(ctx context.Context, index stores.SectorIndex, alloc stores.SectorFileType, ptask sealtasks.TaskType, sector abi.SectorID) (*xjrwSelector, error) {
//	return &xjrwSelector{
//		index:  index,
//		alloc:  alloc,
//		ptask:  ptask,
//		sector: sector,
//	}, nil
//}
//
//func (s *xjrwSelector) Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, whnd *workerHandle) (bool, error) {
//	tasks, err := whnd.w.TaskTypes(ctx)
//	if err != nil {
//		return false, xerrors.Errorf("getting supported worker task types: %w", err)
//	}
//	if _, supported := tasks[task]; !supported {
//		return false, nil
//	}
//
//	inf, err := whnd.w.Info(ctx)
//	if err != nil {
//		return false, xerrors.Errorf("getting worker info: %w", err)
//	}
//	pwk := findSector(stores.SectorName(s.sector), s.ptask)
//	log.Infof("xjrw task = %s s.ptask =%s %v pwk = %s hostname = %s", task, s.ptask, s.sector, pwk, inf.Hostname)
//
//	if len(pwk) > 1 && pwk[0:2] != inf.Hostname[0:2] {
//		return false, nil
//	}
//
//	if task == sealtasks.TTPreCommit1 && pwk != inf.Hostname {
//		return false, nil
//	}
//
//	log.Infof("xjrw passtask = %s s.ptask =%s %v pwk = %s hostname = %s", task, s.ptask, s.sector, pwk, inf.Hostname)
//
//	paths, err := whnd.w.Paths(ctx)
//	if err != nil {
//		return false, xerrors.Errorf("getting worker paths: %w", err)
//	}
//
//	have := map[stores.ID]struct{}{}
//	for _, path := range paths {
//		have[path.ID] = struct{}{}
//	}
//
//	best, err := s.index.StorageFindSector(ctx, s.sector, s.alloc, true)
//	if err != nil {
//		return false, err
//	}
//	for _, info := range best {
//		if _, ok := have[info.ID]; ok {
//			return true, nil
//		}
//	}
//
//	return false, nil
//}
//
//func (s *xjrwSelector) Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) {
//	return a.active.utilization(a.info.Resources) < b.active.utilization(b.info.Resources), nil
//}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
//func (l *LocalWorker) xjrwsb(tsk sealtasks.TaskType) (ffiwrapper.Storage, error) {
//	return ffiwrapper.New(&xjrwLocalWorkerPathProvider{w: l, task: tsk}, l.scfg)
//}
//
func (sb *Manager) WorkerAddPiece(ctx context.Context, sector abi.SectorID, pieceSizes []abi.UnpaddedPieceSize, newPieceSize abi.UnpaddedPieceSize, PiecePath string) (abi.PieceInfo, error) {
	return abi.PieceInfo{}, xerrors.New("Manager xxx")
}

func (m *Manager) AddPiece(ctx context.Context, sector abi.SectorID, existingPieces []abi.UnpaddedPieceSize, sz abi.UnpaddedPieceSize, r io.Reader, PiecePath string) (abi.PieceInfo, error) {
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

	var selector WorkerSelector
	var err error
	if len(existingPieces) == 0 { // new
		selector = newAllocSelector(m.index, stores.FTUnsealed, stores.PathSealing)
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

		p, err := w.WorkerAddPiece(ctx, sector, existingPieces, sz, PiecePath)
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

	selector := newAllocSelector(m.index, stores.FTCache|stores.FTSealed, stores.PathSealing)

	err = m.sched.Schedule(ctx, sector, sealtasks.TTPreCommit1, selector, schedFetch(sector, stores.FTUnsealed, stores.PathSealing, stores.AcquireMove), func(ctx context.Context, w Worker) error {
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
		if os.Getenv("LOTUS_PLDEGE") != "" {
			go stores.ShellExecute(os.Getenv("LOTUS_PLDEGE"))
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

func (l *LocalWorker) WorkerAddPiece(ctx context.Context, sector abi.SectorID, pieceSizes []abi.UnpaddedPieceSize, newPieceSize abi.UnpaddedPieceSize, PiecePath string) (abi.PieceInfo, error) {
	log.Infof("xjrw WorkerAddPiece %v, %v PiecePath = %v pieceSizes = %v", sector, newPieceSize, PiecePath, len(pieceSizes))
	t1 := time.Now()
	defer func() {
		t2 := time.Now()
		log.Infof("xjrw cast WorkerAddPiece %v, %v, %v, %v", sector, t2.Sub(t1), t1, t2)
	}()

	if PiecePath != "" {
		store, err := filestore.NewLocalFileStore(filestore.OsPath(os.Getenv("REAL_ROOT")))
		if err != nil {
			return abi.PieceInfo{}, err
		}

		file, err := store.Open(filestore.Path(PiecePath))
		if err != nil {
			return abi.PieceInfo{}, err
		}

		paddedReader, paddedSize := padreader.New(file, uint64(file.Size()))
		log.Infof("paddedSize = %v newPieceSize %v size = %v ", paddedSize, newPieceSize, file.Size())
		return l.AddPiece(ctx, sector, pieceSizes, paddedSize, paddedReader, PiecePath)
	} else {
		r := io.LimitReader(&nr.Reader{}, int64(newPieceSize))
		return l.AddPiece(ctx, sector, pieceSizes, newPieceSize, r, "")
	}
}

//
////func (l *LocalWorker) AddPiece(ctx context.Context, sector abi.SectorID, epcs []abi.UnpaddedPieceSize, sz abi.UnpaddedPieceSize, r io.Reader) (abi.PieceInfo, error) {
////	t1 := time.Now()
////	defer func() {
////		t2 := time.Now()
////		log.Infof("xjrw cast AddPiece %v, %v, %v, %v", sector, t2.Sub(t1), t1, t2)
////	}()
////	//sb, err := l.sb()
////	sb, err := l.xjrwsb(sealtasks.TTAddPiece)
////	if err != nil {
////		return abi.PieceInfo{}, err
////	}
////
////	return sb.AddPiece(ctx, sector, epcs, sz, r)
////}
////
//func (l *LocalWorker) SealPreCommit1(ctx context.Context, sector abi.SectorID, ticket abi.SealRandomness, pieces []abi.PieceInfo) (out storage2.PreCommit1Out, err error) {
//	t1 := time.Now()
//	defer func() {
//		t2 := time.Now()
//		log.Infof("xjrw cast SealPreCommit1 %v, %v, %v, %v", sector, t2.Sub(t1), t1, t2)
//	}()
//	{
//		// cleanup previous failed attempts if they exist
//		if err := l.storage.Remove(ctx, sector, stores.FTSealed, true); err != nil {
//			return nil, xerrors.Errorf("cleaning up sealed data: %w", err)
//		}
//
//		if err := l.storage.Remove(ctx, sector, stores.FTCache, true); err != nil {
//			return nil, xerrors.Errorf("cleaning up cache data: %w", err)
//		}
//	}
//
//	sb, err := l.sb()
//	if err != nil {
//		return nil, err
//	}
//
//	return sb.SealPreCommit1(ctx, sector, ticket, pieces)
//}
//
////func (l *LocalWorker) SealPreCommit2(ctx context.Context, sector abi.SectorID, phase1Out storage2.PreCommit1Out) (cids storage2.SectorCids, err error) {
////	t1 := time.Now()
////	defer func() {
////		t2 := time.Now()
////		log.Infof("xjrw cast SealPreCommit2 %v, %v, %v, %v", sector, t2.Sub(t1), t1, t2)
////	}()
////
////	sb, err := l.xjrwsb(sealtasks.TTPreCommit2)
////	if err != nil {
////		return storage2.SectorCids{}, err
////	}
////
////	return sb.SealPreCommit2(ctx, sector, phase1Out)
////}
////
////func (l *LocalWorker) SealPreCommit2(ctx context.Context, sector abi.SectorID, phase1Out storage2.PreCommit1Out) (cids storage2.SectorCids, err error) {
////	t1 := time.Now()
////	defer func() {
////		t2 := time.Now()
////		log.Infof("xjrw cast SealPreCommit2 %v, %v, %v, %v", sector, t2.Sub(t1), t1, t2)
////	}()
////	// xjrw modify
////	// cp文件到自己的工作目录, 然后移除远程文件，再申明文件在本地
////	bgp := stores.GetSectorBeegfsPath(sector)
////	lcp := stores.GetSectorLocalPath(sector)
////
////	cerr := stores.ShellExecute(fmt.Sprintf("cp -r %s %s", bgp.Sealed, lcp.Sealed))
////	log.Infof("xjrw copy sealed file %s -> %s  %v", bgp.Sealed, lcp.Sealed, cerr)
////	cerr = stores.ShellExecute(fmt.Sprintf("cp -r %s %s", bgp.Cache, lcp.Cache))
////	log.Infof("xjrw copy cache file %s -> %s  %v", bgp.Cache, lcp.Cache, cerr)
////
////	t3 := time.Now()
////	log.Infof("xjrw copy cast %v, %v, %v, %v", sector, t3.Sub(t1), t1, t3)
////
////	//sb, err := l.sb()
////	sb, err := l.xjrwsb(sealtasks.TTPreCommit2)
////	if err != nil {
////		return storage2.SectorCids{}, err
////	}
////
////	return sb.SealPreCommit2(ctx, sector, phase1Out)
////}
////
////func (l *LocalWorker) SealCommit1(ctx context.Context, sector abi.SectorID, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, pieces []abi.PieceInfo, cids storage2.SectorCids) (output storage2.Commit1Out, err error) {
////	t1 := time.Now()
////	defer func() {
////		t2 := time.Now()
////		log.Infof("xjrw cast SealCommit1 %v, %v, %v, %v", sector, t2.Sub(t1), t1, t2)
////	}()
////	return l.oldSealCommit1(ctx, sector, ticket, seed, pieces, cids)
////}
////
////func (l *LocalWorker) SealCommit2(ctx context.Context, sector abi.SectorID, phase1Out storage2.Commit1Out) (proof storage2.Proof, err error) {
////	t1 := time.Now()
////	defer func() {
////		t2 := time.Now()
////		log.Infof("xjrw cast SealCommit2 %v, %v, %v, %v", sector, t2.Sub(t1), t1, t2)
////	}()
////	return l.oldSealCommit2(ctx, sector, phase1Out)
////}
////
////func (l *LocalWorker) FinalizeSector(ctx context.Context, sector abi.SectorID, keepUnsealed []storage2.Range) error {
////	t1 := time.Now()
////	defer func() {
////		t2 := time.Now()
////		log.Infof("xjrw cast FinalizeSector %v, %v, %v, %v", sector, t2.Sub(t1), t1, t2)
////	}()
////
////	//sb, err := l.sb()
////	sb, err := l.xjrwsb(sealtasks.TTFinalize)
////	if err != nil {
////		return err
////	}
////
////	if err := sb.FinalizeSector(ctx, sector, keepUnsealed); err != nil {
////		return xerrors.Errorf("finalizing sector: %w", err)
////	}
////
////	return nil
////}
