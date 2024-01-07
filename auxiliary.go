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
			worker_info := &(WorkerAll[i])
			fmt.Println("AllConnectionInfoStatus:", AllConnectionInfoStatus)
			fmt.Printf("worker_info.UpstreamCurConnCount: %v\n", worker_info.UpstreamCurConnCount)
		}
		time.Sleep(5 * time.Second)
	}
}
