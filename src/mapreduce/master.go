package mapreduce

import "container/list"
import "fmt"

import "sync/atomic"

type WorkerInfo struct {
	address string
	// You can add definitions here.
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
	var iMap, iReduce = 0, 0

	// scheduler loop
	for workerName := range mr.registerChannel {
		DPrintf("RunMaster: %s\n", workerName)
		if _, ok := mr.Workers[workerName]; !ok {
			mr.Workers[workerName] = &WorkerInfo{
				address: workerName,
			}
			mr.nWorker++
		}
		
		// assign map work
		if iMap != mr.nMap {
			DPrintf("Map phase\n")
			go func(workerName string, mr *MapReduce, num int) {
				arg := &DoJobArgs{
					JobNumber: num,
					File: mr.file,
					NumOtherPhase: mr.nReduce,
					Operation: Map,
				}
				var reply DoJobReply
				ok := false
				for !ok {
					ok = call(workerName, "Worker.DoJob", arg, &reply)
				}
				atomic.AddInt32(&mr.nMapDone, 1)
				// mr.nMapDone++
				mr.registerChannel <- workerName
			}(workerName, mr, iMap)
			iMap++
		// assign reduce work
		} else if iReduce != mr.nReduce {
			DPrintf("Reduce phase\n")
			go func(workerName string, mr *MapReduce, num int) {
				arg := &DoJobArgs{
					JobNumber: num,
					File: mr.file,
					NumOtherPhase: mr.nMap,
					Operation: Reduce,
				}
				var reply DoJobReply
				
				// wait all map job finish
				for int(mr.nMapDone) != mr.nMap {}
				ok := false
				for !ok {
					ok = call(workerName, "Worker.DoJob", arg, &reply)
				}
				atomic.AddInt32(&mr.nReduceDone, 1)
				// mr.nReduceDone++
				mr.registerChannel <- workerName
			}(workerName, mr, iReduce)
			iReduce++
		} else {
			break
		}
	}
	
	// reduce phase
	// call(worker, "Worker.DoJob", arg, &reply)
	
	return mr.KillWorkers()
}
