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

type A struct {
	m int
	n int
}

// func test1(a chan<- NewConnectionInfo) {
// 	for {
// 		c := ConnectionInfo{
// 			ListenFd: 3,
// 		}
// 		fmt.Printf("c: %p\n", &c)
// 		d := NewConnectionInfo{CliFd: FD(2),
// 			ConnectionInfo: c,
// 		}
// 		fmt.Printf("d.ConnectionInfo: %p\n", &(d.ConnectionInfo))

// 		a <- d
// 		time.Sleep(1 * time.Second)
// 	}
// }

// func test2(b <-chan NewConnectionInfo) {
// 	// var c []*ConnectionInfo = make([]*ConnectionInfo, 0)
// 	var item  NewConnectionInfo
// 	for item = range b {
// 		// c = append(c, &item.ConnectionInfo)
// 		fmt.Printf("item.ConnectionInfo: %p\n", item.ConnectionInfo)
// 	}
// }

func my_temp_test() {
	// do something
	// a := make(chan NewConnectionInfo, 0)

	// go test1(a)
	// test2(a)

	// unix.Exit(0)
}
