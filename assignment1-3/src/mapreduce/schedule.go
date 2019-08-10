package mapreduce

import (
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)
	var wg sync.WaitGroup
	wg.Add(ntasks)
	avail := make(chan string, ntasks)
	for i := 0; i < ntasks; i++ {
		var file string
		if phase == mapPhase {
			file = mr.files[i]
		}
		go func(i int) {
			for {
				var server string
				select {
				case server = <-mr.registerChannel:
				case server = <-avail:
				}
				args := DoTaskArgs{
					JobName:       mr.jobName,
					File:          file,
					Phase:         phase,
					TaskNumber:    i,
					NumOtherPhase: nios,
				}
				ok := call(server, "Worker.DoTask", args, new(struct{}))
				if ok {
					avail <- server
					debug("Schedule: task %d Done\n", i)
					break
				}
				debug("Schedule: task %v failed on %s\n", args, server)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	// return used servers to the registration channel as space becomes available in it
	go func() {
		for server := range avail {
			mr.registerChannel <- server
		}
	}()

	debug("Schedule: %v phase done\n", phase)
}
