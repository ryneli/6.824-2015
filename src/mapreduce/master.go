package mapreduce

import "container/list"
import "fmt"

import "sync/atomic"

type WorkerInfo struct {
	address string
	// You can add definitions here.
	jobId   int
	success bool
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	// map loop
	mapFunc := func(workerName string, mr *MapReduce, num int) {
		arg := &DoJobArgs{
			JobNumber: num,
			File: mr.file,
			NumOtherPhase: mr.nReduce,
			Operation: Map,
		}
		var reply DoJobReply
		mr.Workers[workerName] = &WorkerInfo{
			address:workerName,
			jobId: num,
			success: call(workerName, "Worker.DoJob", arg, &reply),
		}
		atomic.AddInt32(&mr.nMapDone, 1)
		mr.registerChannel <- workerName
	}

	reduceFunc := func(workerName string, mr *MapReduce, num int) {
		arg := &DoJobArgs{
			JobNumber: num,
			File: mr.file,
			NumOtherPhase: mr.nMap,
			Operation: Reduce,
		}
		var reply DoJobReply
		
		mr.Workers[workerName] = &WorkerInfo{
			address: workerName,
			jobId: num,
			success: call(workerName, "Worker.DoJob", arg, &reply),
		}
		atomic.AddInt32(&mr.nReduceDone, 1)
		mr.registerChannel <- workerName
	}
	
	freeWorker := make([]string, 0)
	mapTasks, mapDone := make([]int, mr.nMap), list.New()
	for i := 0; i < mr.nMap; i++ {
		mapTasks[i] = i
	}

	for mapDone.Len() != mr.nMap {
		var workerName string
		ready := true
		if len(freeWorker) > 0 && len(mapTasks) > 0 {
			workerName, freeWorker = freeWorker[len(freeWorker)-1], freeWorker[:len(freeWorker)-1]
		} else {
			workerName = <-mr.registerChannel
			if workerInfo, ok := mr.Workers[workerName]; ok {
				delete(mr.Workers, workerName)
				if workerInfo.success == false {
					mapTasks = append(mapTasks, workerInfo.jobId)
					ready = false
				} else {
					mapDone.PushBack(workerInfo.jobId)
				}
			}
		}
		if !ready {
			if len(freeWorker) > 0 {
				workerName, freeWorker = freeWorker[len(freeWorker)-1], freeWorker[:len(freeWorker)-1]
			} else {
				continue
			}
		}
		
		if len(mapTasks) > 0 {
			var jobId int
			jobId, mapTasks = mapTasks[len(mapTasks)-1], mapTasks[:len(mapTasks)-1]
			go mapFunc(workerName, mr, jobId)
		} else {
			freeWorker = append(freeWorker, workerName)
		}
		fmt.Printf("wait mapDone %d; tasks %d; freeworker %d\n", mapDone.Len(), len(mapTasks), len(freeWorker))
	}

	reduceTasks, reduceDone := make([]int, mr.nReduce), list.New()
	for i := 0; i < mr.nReduce; i++ {
		reduceTasks[i] = i
	}
	for reduceDone.Len() != mr.nReduce {
		var workerName string
		ready := true
		if len(freeWorker) > 0 && len(reduceTasks) > 0 {
			workerName, freeWorker = freeWorker[len(freeWorker)-1], freeWorker[:len(freeWorker)-1]
		} else {
			workerName = <-mr.registerChannel
			if workerInfo, ok := mr.Workers[workerName]; ok {
				delete(mr.Workers, workerName)
				if workerInfo.success == false {
					reduceTasks = append(reduceTasks, workerInfo.jobId)
					ready = false
				} else {
					reduceDone.PushBack(workerInfo.jobId)
				}
			}
		}

		if !ready {
			if len(freeWorker) > 0 {
				workerName, freeWorker = freeWorker[len(freeWorker)-1], freeWorker[:len(freeWorker)-1]
			} else {
				continue
			}
		}

		if len(reduceTasks) > 0 {
			var jobId int
			jobId, reduceTasks = reduceTasks[len(reduceTasks)-1], reduceTasks[:len(reduceTasks)-1]
			go reduceFunc(workerName, mr, jobId)
		} else {
			fmt.Printf("wait reduceDone %d\n", reduceDone.Len())
		}
	}

	return mr.KillWorkers()
}
