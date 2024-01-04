package main

import (
	"fmt"
	"time"
)

func getListenerUpstream() (tcp_listener_upstream_all TcpListenerUpstreamSlice) {
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
	listener_upstream_1 := TcpListenerUpstream{
		Listener: IpPort{
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
	listener_upstream_2 := TcpListenerUpstream{
		Listener: IpPort{
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

func my_temp_test() {
	// do something
	// time_1 := time.Now()
	// time.Sleep(5 * time.Second)
	// time_2 := time.Now()

	// if time_2.Sub(time_1).Seconds() >= 6 {
	// 	fmt.Println(true)
	// } else {
	// 	fmt.Println(false)
	// }

	// unix.Exit(0)
}
