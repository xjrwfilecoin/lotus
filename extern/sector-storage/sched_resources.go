package sectorstorage

import (
	"os"
	"strconv"
	"sync"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

func (a *activeResources) withResources(req *workerRequest, id WorkerID, wr storiface.WorkerResources, r Resources, locker sync.Locker, lk sync.Mutex, cb func() error) error {
	for !a.canHandleRequest(r, id, "withResources", wr, req) {
		if a.cond == nil {
			a.cond = sync.NewCond(locker)
		}
		a.cond.Wait()
	}

	lk.Lock()
	log.Infof("withResources add1 %v %v %v %v %v", req.sector, id, req.taskType, a.gpuUsed, r.CanGPU)
	a.add(wr, r)
	log.Infof("withResources add2 %v %v %v %v %v", req.sector, id, req.taskType, a.gpuUsed, r.CanGPU)
	lk.Unlock()

	err := cb()

	lk.Lock()
	a.free(wr, r)
	lk.Unlock()
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

func (a *activeResources) canHandleRequest(needRes Resources, wid WorkerID, caller string, res storiface.WorkerResources, req *workerRequest) bool {
	log.Infof("canHandleRequest start %v %v %v %v %v %v %v", req.sector, wid, req.taskType, caller, len(res.GPUs), needRes.CanGPU, a.gpuUsed)
	if p1Str := os.Getenv("P1_LIMIT"); p1Str != "" {
		if p1Num, err := strconv.Atoi(p1Str); err == nil && req.taskType == sealtasks.TTPreCommit1 && needRes.MaxMemory != 0 {
			if a.memUsedMax/needRes.MaxMemory < uint64(p1Num) {
				return true
			}
			log.Infof("P1 canHandleRequest limit %v %v %v %v", a.memUsedMax, needRes.MaxMemory, a.memUsedMax/needRes.MaxMemory, p1Num)
			return false
		}
	}

	if c2Str := os.Getenv("C2_LIMIT"); c2Str != "" {
		if c2Num, err := strconv.Atoi(c2Str); err == nil && req.taskType == sealtasks.TTCommit2 && needRes.MaxMemory != 0 {
			if a.memUsedMax/needRes.MaxMemory < uint64(c2Num) && a.memUsedMax%needRes.MaxMemory == 0 {
				log.Infof("C2_LIMIT %v %v %v %v %v %v", a.memUsedMax, needRes.MaxMemory, c2Num, a.memUsedMax/needRes.MaxMemory, req.sector, caller)
				return true
			}
			log.Infof("C2 canHandleRequest limit %v %v %v %v %v %v", a.memUsedMax, needRes.MaxMemory, c2Num, a.memUsedMax/needRes.MaxMemory, req.sector, caller)
			return false
		}
	}
	// TODO: dedupe needRes.BaseMinMemory per task type (don't add if that task is already running)
	minNeedMem := res.MemReserved + a.memUsedMin + needRes.MinMemory + needRes.BaseMinMemory
	if minNeedMem > res.MemPhysical {
		log.Debugf("sched: not scheduling on worker %d for %s; not enough physical memory - need: %dM, have %dM %v %v", wid, caller, minNeedMem/mib, res.MemPhysical/mib, req.sector, req.taskType)
		return false
	}

	maxNeedMem := res.MemReserved + a.memUsedMax + needRes.MaxMemory + needRes.BaseMinMemory

	if maxNeedMem > res.MemSwap+res.MemPhysical {
		log.Debugf("sched: not scheduling on worker %d for %s; not enough virtual memory - need: %dM, have %dM %v %v", wid, caller, maxNeedMem/mib, (res.MemSwap+res.MemPhysical)/mib, req.sector, req.taskType)
		return false
	}

	if a.cpuUse+needRes.Threads(res.CPUs) > res.CPUs {
		log.Debugf("sched: not scheduling on worker %d for %s; not enough threads, need %d, %d in use, target %d %v %v", wid, caller, needRes.Threads(res.CPUs), a.cpuUse, res.CPUs, req.sector, req.taskType)
		return false
	}

	if len(res.GPUs) > 0 && needRes.CanGPU {
		if a.gpuUsed {
			log.Debugf("sched: not scheduling on worker %d for %s; GPU in use %v %v", wid, caller, req.sector, req.taskType)
			return false
		}
	}
	log.Infof("canHandleRequest end %v %v %v %v %v %v %v %v", req.sector, wid, req.taskType, caller, res, needRes, a)

	return true
}

func (a *activeResources) utilization(wr storiface.WorkerResources) float64 {
	var max float64

	cpu := float64(a.cpuUse) / float64(wr.CPUs)
	max = cpu

	memMin := float64(a.memUsedMin+wr.MemReserved) / float64(wr.MemPhysical)
	if memMin > max {
		max = memMin
	}

	memMax := float64(a.memUsedMax+wr.MemReserved) / float64(wr.MemPhysical+wr.MemSwap)
	if memMax > max {
		max = memMax
	}

	return max
}

func (wh *workerHandle) utilization() float64 {
	wh.lk.Lock()
	u := wh.active.utilization(wh.info.Resources)
	u += wh.preparing.utilization(wh.info.Resources)
	wh.lk.Unlock()
	wh.wndLk.Lock()
	for _, window := range wh.activeWindows {
		u += window.allocated.utilization(wh.info.Resources)
	}
	wh.wndLk.Unlock()

	return u
}
