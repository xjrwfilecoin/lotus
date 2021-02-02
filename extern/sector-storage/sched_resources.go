package sectorstorage

import (
	"sync"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

func (a *activeResources) withResources(taskDone chan struct{}, req *workerRequest, worker *workerHandle, id WorkerID, wr storiface.WorkerResources, r Resources, locker sync.Locker, cb func() error) error {
	for !a.canHandleRequest(r, id, "withResources", wr, req, worker) {
		if a.cond == nil {
			a.cond = sync.NewCond(locker)
		}
		a.cond.Wait()
	}

	log.Infof("start %v %v %s", req.sector, req.taskType, id)
	if req.taskType == sealtasks.TTPreCommit2 {
		worker.p2Running[req.sector.ID] = struct{}{}
		log.Info("p2Running add", worker.p2Running)
	} else if req.taskType == sealtasks.TTCommit2 {
		worker.c2Running[req.sector.ID] = struct{}{}
		log.Info("c2Running add", worker.c2Running)
	} else if req.taskType == sealtasks.TTPreCommit1 {
		worker.p1Running[req.sector.ID] = struct{}{}
		log.Info("p1Running add", worker.p1Running)
	} else if req.taskType == sealtasks.TTAddPiece {
		worker.addPieceRuning[req.sector.ID] = struct{}{}
		log.Info("addPieceRuning add", worker.addPieceRuning)
	}

	a.add(wr, r)

	err := cb()

	log.Infof("finish %v %v %v", req.sector, req.taskType, id)
	if req.taskType == sealtasks.TTPreCommit2 {
		delete(worker.p2Running, req.sector.ID)
		log.Info("p2Running del", worker.p2Running)
	} else if req.taskType == sealtasks.TTCommit2 {
		delete(worker.c2Running, req.sector.ID)
		log.Info("c2Running del", worker.c2Running)
	} else if req.taskType == sealtasks.TTPreCommit1 {
		delete(worker.p1Running, req.sector.ID)
		log.Info("p1Running del", worker.p1Running)
	} else if req.taskType == sealtasks.TTAddPiece {
		delete(worker.addPieceRuning, req.sector.ID)
		log.Info("addPieceRuning del", worker.addPieceRuning)
	}

	a.free(wr, r)

	go func() {
		log.Infof("taskDone %v %v", req.sector, req.taskType)
		select {
		case taskDone <- struct{}{}:
		}
	}()

	if a.cond != nil {
		a.cond.Broadcast()
	}

	return err
}

func (a *activeResources) add(wr storiface.WorkerResources, r Resources) {
	if r.CanGPU {
		a.gpuUsed = true
	}
	a.cpuUse += r.Threads(wr.CPUs)
	a.memUsedMin += r.MinMemory
	a.memUsedMax += r.MaxMemory
}

func (a *activeResources) free(wr storiface.WorkerResources, r Resources) {
	if r.CanGPU {
		a.gpuUsed = false
	}
	a.cpuUse -= r.Threads(wr.CPUs)
	a.memUsedMin -= r.MinMemory
	a.memUsedMax -= r.MaxMemory
}

func (a *activeResources) canHandleRequest(needRes Resources, wid WorkerID, caller string, res storiface.WorkerResources, req *workerRequest, worker *workerHandle) bool {
	if worker.enabled == false {
		log.Infof("canHandleRequest enable %v %v %v %v %v %v %v", req.sector, wid, req.taskType, caller, len(res.GPUs), needRes.CanGPU, a.gpuUsed)
		//return false
	}
	//log.Infof("canHandleRequest start %v %v %v %v %v %v %v %v %v %v", req.sector, wid, req.taskType, caller, len(res.GPUs), needRes.CanGPU, a.gpuUsed, len(worker.p1Running), len(worker.c2Running), len(worker.p2Running))

	if req.taskType == sealtasks.TTCommit1 || req.taskType == sealtasks.TTAddPiece || req.taskType == sealtasks.TTFetch || req.taskType == sealtasks.TTFinalize {
		//log.Infof("%v TTCommit1&TTAddPiece %v %v", req.sector, req.taskType, caller)
		return true
	}

	if req.taskType == sealtasks.TTPreCommit1 && p1Limit > 0 {
		if len(worker.p1Running) < p1Limit {
			//log.Infof("P1_LIMIT %v %v %v %v %v", wid, a.cpuUse, a.gpuUsed, p1Limit, worker.p1Running)
			return true
		} else {
			//log.Infof("P1_LIMIT exceed %v %v %v %v %v", wid, a.cpuUse, a.gpuUsed, p1Limit, worker.p1Running)
			return false
		}
	}

	if req.taskType == sealtasks.TTCommit2 && c2Limit > 0 {
		if len(worker.c2Running) < c2Limit {
			//log.Infof("C2_LIMIT %v %v %v %v %v", wid, a.cpuUse, a.gpuUsed, c2Limit, worker.c2Running)
			return true
		} else {
			//log.Infof("C2_LIMIT exceed %v %v %v %v %v", wid, a.cpuUse, a.gpuUsed, c2Limit, worker.c2Running)
			return false
		}
	}

	if req.taskType == sealtasks.TTPreCommit2 && p2Limit > 0 {
		if len(worker.p2Running) < p2Limit {
			//log.Infof("P2_LIMIT %v %v %v %v %v", wid, a.cpuUse, a.gpuUsed, p2Limit, worker.p2Running)
			return true
		} else {
			//log.Infof("P2_LIMIT exceed %v %v %v %v %v", wid, a.cpuUse, a.gpuUsed, p2Limit, worker.p2Running)
			return false
		}
	}
	// TODO: dedupe needRes.BaseMinMemory per task type (don't add if that task is already running)
	minNeedMem := res.MemReserved + a.memUsedMin + needRes.MinMemory + needRes.BaseMinMemory
	if minNeedMem > res.MemPhysical {
		log.Debugf("sched: not scheduling on worker %s for %s; not enough physical memory - need: %dM, have %dM %v %v", wid, caller, minNeedMem/mib, res.MemPhysical/mib, req.sector, req.taskType)
		return false
	}

	maxNeedMem := res.MemReserved + a.memUsedMax + needRes.MaxMemory + needRes.BaseMinMemory

	if maxNeedMem > res.MemSwap+res.MemPhysical {
		log.Debugf("sched: not scheduling on worker %s for %s; not enough virtual memory - need: %dM, have %dM %v %v", wid, caller, maxNeedMem/mib, (res.MemSwap+res.MemPhysical)/mib, req.sector, req.taskType)
		return false
	}

	if a.cpuUse+needRes.Threads(res.CPUs) > res.CPUs {
		log.Debugf("sched: not scheduling on worker %s for %s; not enough threads, need %d, %d in use, target %d %v %v", wid, caller, needRes.Threads(res.CPUs), a.cpuUse, res.CPUs, req.sector, req.taskType)
		return false
	}

	if len(res.GPUs) > 0 && needRes.CanGPU {
		if a.gpuUsed {
			log.Debugf("sched: not scheduling on worker %s for %s; GPU in use %v %v", wid, caller, req.sector, req.taskType)
			return false
		}
	}
	//log.Infof("canHandleRequest end %v %v %v %v %v %v %v %v %v %v %v", req.sector, wid, req.taskType, caller, res, needRes, a, len(worker.p1Running), len(worker.c2Running), len(worker.p2Running))

	return true
}

func (a *activeResources) utilization(wr storiface.WorkerResources) float64 {
	var max float64

	cpu := float64(a.cpuUse) / float64(wr.CPUs)
	max = cpu

	//memMin := float64(a.memUsedMin+wr.MemReserved) / float64(wr.MemPhysical)
	//if memMin > max {
	//	max = memMin
	//}
	//
	//memMax := float64(a.memUsedMax+wr.MemReserved) / float64(wr.MemPhysical+wr.MemSwap)
	//if memMax > max {
	//	max = memMax
	//}

	return max
}

func (wh *workerHandle) utilization() float64 {
	wh.lk.Lock()
	u := wh.active.utilization(wh.info.Resources)
	//u += wh.preparing.utilization(wh.info.Resources)
	wh.lk.Unlock()
	//wh.wndLk.Lock()
	//for _, window := range wh.activeWindows {
	//	u += window.allocated.utilization(wh.info.Resources)
	//}
	//wh.wndLk.Unlock()

	log.Infof("utilization %v %v", wh.info.Hostname, u)

	return u
}
