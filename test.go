package main

import (
	"fmt"
	"time"
)

func getListenerUpstream() (tcp_listener_upstream_all TcpListenIpPortUpstreamSlice) {
	upstream_1 := UpStreamSlice{
		{
			IpPort: IpPort{
				Ip:   [4]byte{10, 5, 5, 20},
				Port: 28080,
			},
			IsHealthy:         true,
			HealthyUpdateTime: time.Now(),
		},
		{
			IpPort: IpPort{
				Ip:   [4]byte{10, 5, 5, 20},
				Port: 18080,
			},
			IsHealthy:         true,
			HealthyUpdateTime: time.Now(),
		},
	}
	listener_upstream_1 := TcpListenIpPortUpstream{
		IpPortListen: IpPort{
			Port: 30880,
			Ip:   [4]byte{0, 0, 0, 0},
		},
		Upstream: &upstream_1,
	}

	upstream_2 := UpStreamSlice{
		{
			IpPort: IpPort{
				Ip:   [4]byte{10, 5, 5, 21},
				Port: 22,
			},
			IsHealthy:         true,
			HealthyUpdateTime: time.Now(),
		},
		{
			IpPort: IpPort{
				Ip:   [4]byte{10, 5, 5, 20},
				Port: 22,
			},
			IsHealthy:         true,
			HealthyUpdateTime: time.Now(),
		},
	}
	listener_upstream_2 := TcpListenIpPortUpstream{
		IpPortListen: IpPort{
			Port: 30222,
			Ip:   [4]byte{0, 0, 0, 0},
		},
		Upstream: &upstream_2,
	}

	tcp_listener_upstream_all = append(tcp_listener_upstream_all, listener_upstream_1)
	tcp_listener_upstream_all = append(tcp_listener_upstream_all, listener_upstream_2)

	return
}

func test_compare2Maps() {
	old := getListenerUpstream()
	// TODO
	new := getListenerUpstream()

	fmt.Println("old:", old)
	fmt.Println("new:", new)

	add, del := old.compare2tcp_listener_upstream_all(new)

	fmt.Println("add:", add)
	fmt.Println("del:", del)

}

var a []int = make([]int, 2)

const count = 100

var A []*int = make([]*int, count)
var AA []*[]*int = make([]*[]*int, count)

// var B map[int]*int = make(map[int]*int, count)
// var B map[int]*int

func test1() {
	last := -1
	for m := 0; m < count*count; m++ {
		n := m / count
		j := m % count
		if n > last {
			a := make([]*int, count)
			AA[n] = &a
			last = n
		}
		(*AA[n])[j] = new(int)
		*(*AA[n])[j] = m
	}
}

func test2() {
	for i := 0; i < count; i++ {
		A[i] = new(int)
	}
}

func test11() {
	for i := 0; i < 10; i++ {
		A[i+count] = new(int)
	}
}

// func test3() {
// 	for i := 0; i < count; i++ {
// 		B[i] = new(int)
// 	}
// }

// func test4() {
// 	for i := 0; i < count; i++ {
// 		B[i] = new(int)
// 	}
// }

func my_temp_test() {
	// do something
	// a := make(chan NewConnectionInfo, 0)

	// test1()

	// for m := 0; m < count*count; m++ {
	// 	n := m / count
	// 	j := m % count
	// 	fmt.Printf("n: %d, j: %d, m: %d, %p\n", n, j, m, (*AA[n])[j])
	// }

	// unix.Exit(0)
}
