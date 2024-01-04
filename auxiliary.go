package main

import (
	"fmt"
	"runtime"
	"time"
)

func printGoroutineInfo() {
	for {
		fmt.Println("go routine num:", runtime.NumGoroutine(), "worker num:", WORKERCOUNT, "runtime.NumCPU():", runtime.NumCPU())
		for i := 0; i < WORKERCOUNT; i++ {
			worker_info := &(WorkerInfoAll[i])
			worker_info.mutex_connCliUpstreamInfo.RLock()
			fmt.Println("worker_info.connCliUpstreamInfo:", worker_info.connCliUpstreamInfo)
			worker_info.mutex_connCliUpstreamInfo.RUnlock()

			worker_info.mutex_UpstreamCurConnCount.RLock()
			fmt.Printf("worker_info.UpstreamCurConnCount: %v\n", worker_info.UpstreamCurConnCount)
			worker_info.mutex_UpstreamCurConnCount.RUnlock()
		}
		time.Sleep(5 * time.Second)
	}
}
