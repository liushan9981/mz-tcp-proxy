package main

import (
	"fmt"
	"time"

	"golang.org/x/sys/unix"
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

const count = 123456

var A []*int = make([]*int, count)
var B map[int]*int = make(map[int]*int, count)

func test1() {
	for i := 0; i < count; i++ {
		A[i] = new(int)
	}
}

func test2() {
	for i := 0; i < count; i++ {
		A[i] = new(int)
	}
}

func test3() {
	for i := 0; i < count; i++ {
		B[i] = new(int)
	}
}

func test4() {
	for i := 0; i < count; i++ {
		B[i] = new(int)
	}
}

func my_temp_test() {
	// do something
	// a := make(chan NewConnectionInfo, 0)

	go test3()
	test4()

	for i := 0; i < count; i++ {
		fmt.Printf("%p\n", B[i])
	}

	unix.Exit(0)
}
