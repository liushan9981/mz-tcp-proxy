package main

import "fmt"

func getListenerUpstream() (tcp_listener_upstream_all []TcpListenerUpstream) {
	upstream_1 := []IpPort{
		{
			Ip:   [4]byte{10, 5, 5, 20},
			Port: 28080,
		},
		{
			Ip:   [4]byte{10, 5, 5, 20},
			Port: 18080,
		},
	}
	listener_upstream_1 := TcpListenerUpstream{
		Listener: IpPort{
			Port: 30880,
			Ip:   [4]byte{0, 0, 0, 0},
		},
		Upstream: upstream_1,
	}

	upstream_2 := []IpPort{
		{
			Ip:   [4]byte{10, 5, 5, 21},
			Port: 22,
		},
		{
			Ip:   [4]byte{10, 5, 5, 20},
			Port: 22,
		},
	}
	listener_upstream_2 := TcpListenerUpstream{
		Listener: IpPort{
			Port: 30222,
			Ip:   [4]byte{0, 0, 0, 0},
		},
		Upstream: upstream_2,
	}

	tcp_listener_upstream_all = append(tcp_listener_upstream_all, listener_upstream_1)
	tcp_listener_upstream_all = append(tcp_listener_upstream_all, listener_upstream_2)

	return
}

func getListenerUpstreamNew() (tcp_listener_upstream_all []TcpListenerUpstream) {
	upstream_1 := []IpPort{
		{
			Ip:   [4]byte{10, 5, 5, 20},
			Port: 28080,
		},
		{
			Ip:   [4]byte{10, 5, 5, 20},
			Port: 18080,
		},
	}
	listener_upstream_1 := TcpListenerUpstream{
		Listener: IpPort{
			Port: 30880,
			Ip:   [4]byte{0, 0, 0, 0},
		},
		Upstream: upstream_1,
	}

	// upstream_2 := []IpPort{
	// 	{
	// 		Ip:   [4]byte{10, 5, 5, 21},
	// 		Port: 2222,
	// 	},
	// 	{
	// 		Ip:   [4]byte{10, 5, 5, 20},
	// 		Port: 22,
	// 	},
	// }
	// listener_upstream_2 := TcpListenerUpstream{
	// 	Listener: IpPort{
	// 		Port: 30222,
	// 		Ip:   [4]byte{0, 0, 0, 0},
	// 	},
	// 	Upstream: upstream_2,
	// }

	upstream_3 := []IpPort{
		{
			Ip:   [4]byte{10, 5, 5, 21},
			Port: 22,
		},
		{
			Ip:   [4]byte{10, 5, 5, 20},
			Port: 22,
		},
	}
	listener_upstream_3 := TcpListenerUpstream{
		Listener: IpPort{
			Port: 30223,
			Ip:   [4]byte{0, 0, 0, 0},
		},
		Upstream: upstream_3,
	}

	tcp_listener_upstream_all = append(tcp_listener_upstream_all, listener_upstream_1)
	// tcp_listener_upstream_all = append(tcp_listener_upstream_all, listener_upstream_2)
	tcp_listener_upstream_all = append(tcp_listener_upstream_all, listener_upstream_3)

	return
}

func test_compare2Maps() {
	old := getListenerUpstream()
	new := getListenerUpstreamNew()

	fmt.Println("old:", old)
	fmt.Println("new:", new)

	add, del := compare2tcp_listener_upstream_all(old, new)

	fmt.Println("add:", add)
	fmt.Println("del:", del)

}
