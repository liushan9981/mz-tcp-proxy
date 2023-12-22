package main

import (
	"fmt"
	"log"
	"sync"

	"golang.org/x/sys/unix"
)

const LISTENQUEUECOUNT = 1024
const WORKERCOUNT = 8
const RECV_SIZE = 4096
const CLI_IN_DATA = "CLI_IN_DATA"
const UPSTREAM_IN_DATA = "UPSTREAM_IN_DATA"

type ConnectionReadStatus struct {
	ReadCliEOF      bool
	ReadUpstreamEOF bool
}

type ConnectionFdReadStatus struct {
	RequestId  string
	ListenFd   int
	ListenPort int
	Upstream   IpPort
	Fd         int
	SA         unix.Sockaddr
	ConnectionReadStatus
}

type WorkerInfo struct {
	EpFd                 int
	connCli2UpstreamInfo *(map[int]*ConnectionFdReadStatus)
	connUpstream2CliInfo *(map[int]*ConnectionFdReadStatus)
	mutex                sync.RWMutex
	events               []unix.EpollEvent
	buf                  []byte
	pbuf                 []byte
	goroutineIndex       int
	NewConnectionAll     chan NewConn
}

type NewConn struct {
	ListenFd   int
	ListenPort int
	CliFd      int
	SA         unix.Sockaddr
	Upstream   *([]IpPort)
}

type IpPort struct {
	Ip   [4]byte
	Port int
}

type TcpListenerUpstream struct {
	Listener IpPort
	Upstream []IpPort
}

var (
	WorkerInfoAll [WORKERCOUNT]WorkerInfo
)

func InitWorkerInfoAll() {
	for i := 0; i < WORKERCOUNT; i++ {
		epfd, err := unix.EpollCreate1(0)
		if err != nil {
			log.Fatal(err)
		}
		worker_info := &(WorkerInfoAll[i])
		worker_info.EpFd = epfd
		conn_cli_upstream_info := make(map[int]*ConnectionFdReadStatus, LISTENQUEUECOUNT)
		conn_upstream_cli_info := make(map[int]*ConnectionFdReadStatus, LISTENQUEUECOUNT)
		worker_info.connCli2UpstreamInfo = &conn_cli_upstream_info
		worker_info.connUpstream2CliInfo = &conn_upstream_cli_info
		worker_info.mutex = sync.RWMutex{}
		worker_info.events = make([]unix.EpollEvent, LISTENQUEUECOUNT)
		worker_info.buf = make([]byte, RECV_SIZE)
		worker_info.pbuf = make([]byte, RECV_SIZE)
		worker_info.NewConnectionAll = make(chan NewConn, LISTENQUEUECOUNT)
		worker_info.goroutineIndex = i
	}
}

func AddEpoll(epfd int, fd int) {
	err := unix.SetNonblock(fd, true)
	if err != nil {
		fmt.Println("unix.SetNonblock:", err)
	}
	err = unix.EpollCtl(epfd, unix.EPOLL_CTL_ADD, fd, &unix.EpollEvent{
		Events: unix.EPOLLIN | unix.EPOLLET,
		Fd:     int32(fd),
	})
	if err != nil {
		log.Fatal("AddEpoll error:", err)
	}
}

func DelEpoll(epfd int, fd int) {
	err := unix.EpollCtl(epfd, unix.EPOLL_CTL_DEL, fd, &unix.EpollEvent{
		Events: unix.EPOLLIN | unix.EPOLLET,
		Fd:     int32(fd),
	})
	if err != nil {
		log.Fatal("DelEpoll error:", err)
	}
	// debug
	// fmt.Printf("DelEpoll: epfd: %d, fd: %d\n", epfd, fd)
}

func new_listen(ip_port IpPort) (listen_fd int) {
	listen_fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		log.Fatal(err)
	}
	err = unix.Bind(listen_fd, &unix.SockaddrInet4{
		Port: ip_port.Port,
		Addr: ip_port.Ip,
	})
	if err != nil {
		log.Fatal(err)
	}
	err = unix.Listen(listen_fd, 1024)
	if err != nil {
		log.Fatal(err)
	}
	return
}

func new_listen_all(tcp_listener_upstream_all []TcpListenerUpstream, epfd_listen int) (listenfd_upstream_all map[int]*[]IpPort) {
	listenfd_upstream_all = make(map[int]*[]IpPort, 2)
	for k, _ := range tcp_listener_upstream_all {
		listen_fd := new_listen(tcp_listener_upstream_all[k].Listener)
		AddEpoll(epfd_listen, listen_fd)
		listenfd_upstream_all[listen_fd] = &(tcp_listener_upstream_all[k].Upstream)
	}

	return
}

func StartTCPProxyListen(tcp_listener_upstream_all []TcpListenerUpstream) {
	listen_epfd, err := unix.EpollCreate1(0)
	if err != nil {
		log.Fatal(err)
	}
	defer unix.Close(listen_epfd)

	events_listen := make([]unix.EpollEvent, 1024)
	// log_main_go_routine := "main_go_routine"
	listenfd_upstream_all := new_listen_all(tcp_listener_upstream_all, listen_epfd)

	for {
		// fmt.Println("log_main_go_routine", "begin new epoll-wait")
		nevents, err := unix.EpollWait(listen_epfd, events_listen, -1)

		// handle error
		if err != nil {
			// 重启中断的系统调用
			if err.Error() == "interrupted system call" {
				continue
			} else {
				log.Fatal("my unix.EpollWait: ", err)
			}
		}

		for ev := 0; ev < nevents; ev++ {
			listen_fd := int(events_listen[ev].Fd)
			ListenSock, err := unix.Getsockname(listen_fd)
			if err != nil {
				log.Fatal("error, unix.Getsockname(listen_fd): ", err)
			}
			for {
				cli_fd, sa, err := unix.Accept(listen_fd)
				if err != nil {
					if err == unix.EAGAIN {
						break
					} else {
						fmt.Println("unix.Accept error:", err)
						continue
					}
				}
				goroutineIndex := cli_fd % WORKERCOUNT
				WorkerInfoAll[goroutineIndex].NewConnectionAll <- NewConn{CliFd: cli_fd,
					ListenFd:   listen_fd,
					Upstream:   listenfd_upstream_all[listen_fd],
					SA:         sa,
					ListenPort: ListenSock.(*unix.SockaddrInet4).Port,
				}

			}
		}
	}
}

func StartWorker() {
	for i := 0; i < WORKERCOUNT; i++ {
		go TCPProxyWorker(&WorkerInfoAll[i])
		go HandleNewConnWorker(&WorkerInfoAll[i])
	}
}

func FinRequest(conn_in_out_stream_info, conn_out_in_stream_info *map[int]*ConnectionFdReadStatus, event_in_fd, event_out_fd int, mutex *sync.RWMutex) {
	mutex.Lock()
	fmt.Printf("FIN, reqId: %s\n", (*conn_in_out_stream_info)[event_in_fd].RequestId)
	delete(*conn_out_in_stream_info, event_out_fd)
	delete(*conn_in_out_stream_info, event_in_fd)
	mutex.Unlock()

	unix.Close(event_out_fd)
	unix.Close(event_in_fd)
}

func handleInData(wokerinfo *WorkerInfo, conn_in_out_stream_info, conn_out_in_stream_info *map[int]*ConnectionFdReadStatus, event_in_fd int, in_data_class string) {
	buf := &(wokerinfo.buf)
	pbuf := &(wokerinfo.pbuf)
	epfd := wokerinfo.EpFd
	mutex := &(wokerinfo.mutex)
	mutex.RLock()
	fdOutInfo := (*conn_in_out_stream_info)[event_in_fd]
	mutex.RUnlock()
	event_out_fd := fdOutInfo.Fd

	for {
		n, err := unix.Read(event_in_fd, *buf)

		// Error
		if err != nil {
			if err.Error() == "bad file descriptor" {
				DelEpoll(epfd, event_in_fd)
				DelEpoll(epfd, event_out_fd)
				// 可能有问题，event_in_fd可能在此之前比其他协程使用
				FinRequest(conn_in_out_stream_info, conn_out_in_stream_info, event_in_fd, event_out_fd, mutex)
				break
			} else if err == unix.EAGAIN {
				break
			} else {
				fmt.Printf("read error: in:(%d)  out:%d, err: %v\n", event_in_fd, event_out_fd, err.Error())
			}
		}

		// EOF
		if n == 0 {
			unix.Shutdown(event_out_fd, unix.SHUT_WR)
			DelEpoll(epfd, event_in_fd)

			if in_data_class == CLI_IN_DATA {
				mutex.Lock()
				(*conn_out_in_stream_info)[event_out_fd].ReadCliEOF = true
				mutex.Unlock()

				if fdOutInfo.ReadUpstreamEOF {
					FinRequest(conn_in_out_stream_info, conn_out_in_stream_info, event_in_fd, event_out_fd, mutex)
				}
			} else if in_data_class == UPSTREAM_IN_DATA {
				mutex.Lock()
				(*conn_out_in_stream_info)[event_out_fd].ReadUpstreamEOF = true
				mutex.Unlock()

				if fdOutInfo.ReadCliEOF {
					FinRequest(conn_in_out_stream_info, conn_out_in_stream_info, event_in_fd, event_out_fd, mutex)
				}
			}

			break
		} else if n > 0 {
			// read ok
			*pbuf = (*buf)[0:n]
			write_count, err := unix.Write(event_out_fd, *pbuf)
			if err != nil {
				fmt.Println("write error:", err)
			}
			if write_count != n {
				fmt.Println("warning write_count != n", write_count, n)
			}
		}
	}
}

func TCPProxyWorker(wokerinfo *WorkerInfo) {
	epfd := wokerinfo.EpFd
	mutex := &(wokerinfo.mutex)
	connCli2UpstreamInfo := (wokerinfo.connCli2UpstreamInfo)
	connUpstream2CliInfo := (wokerinfo.connUpstream2CliInfo)
	events := &(wokerinfo.events)

	for {
		nevents, err := unix.EpollWait(epfd, *events, -1)

		if err != nil {
			// 重启中断的系统调用
			if err.Error() == "interrupted system call" {
				continue
			} else {
				log.Fatal("my unix.EpollWait: ", err)
			}
		}

		for ev := 0; ev < nevents; ev++ {
			event_fd := (*events)[ev].Fd
			event_in_fd := int(event_fd)
			var conn_in_out_stream_info *map[int]*ConnectionFdReadStatus
			var conn_out_in_stream_info *map[int]*ConnectionFdReadStatus
			mutex.RLock()
			var in_data_class string
			// 客户端来数据
			_, ok_cli := (*connCli2UpstreamInfo)[event_in_fd]
			// 上游来数据
			_, ok_upstream := (*connUpstream2CliInfo)[event_in_fd]
			mutex.RUnlock()

			if ok_cli {
				conn_in_out_stream_info = connCli2UpstreamInfo
				conn_out_in_stream_info = connUpstream2CliInfo
				in_data_class = CLI_IN_DATA
			} else if ok_upstream {
				conn_in_out_stream_info = connUpstream2CliInfo
				conn_out_in_stream_info = connCli2UpstreamInfo
				in_data_class = UPSTREAM_IN_DATA
			} else {
				mutex.RLock()
				fmt.Println("can not find fd relation, connCli2UpstreamInfo:", connCli2UpstreamInfo,
					"connUpstream2CliInfo:", connUpstream2CliInfo,
					"event_fd_in:", event_in_fd)
				mutex.RUnlock()
				continue
			}
			// 处理单次读事件
			handleInData(wokerinfo, conn_in_out_stream_info, conn_out_in_stream_info, event_in_fd, in_data_class)
		}
	}
}

func HandleNewConnWorker(workerinfo *WorkerInfo) {
	mutex := &(workerinfo.mutex)
	connCli2UpstreamInfo := workerinfo.connCli2UpstreamInfo
	connUpstream2CliInfo := workerinfo.connUpstream2CliInfo
	epfd := workerinfo.EpFd
	upstream_all_req_count := make(map[int]int, 1)

	for new_conn_this_worker := range workerinfo.NewConnectionAll {
		// 负载均衡为轮询
		upstream_all := new_conn_this_worker.Upstream
		listen_fd := new_conn_this_worker.ListenFd
		upstream_all_count, ok := upstream_all_req_count[listen_fd]
		if !ok {
			upstream_all_req_count[listen_fd] = 0
			upstream_all_count = 0
		}

		upstream_i := upstream_all_count % len(*upstream_all)
		upstream_fd := ConnectUpstream((*upstream_all)[upstream_i])
		upstream_all_req_count[listen_fd]++
		reqId := fmt.Sprintf("%d-%d", workerinfo.goroutineIndex, upstream_all_count)
		fmt.Printf("goroutine: %d, NEW reqId: %s, ListenFd: %d, ListenPort: %d, Upstream: %v, %d -> srv -> %d, client: %v\n",
			workerinfo.goroutineIndex,
			reqId,
			listen_fd,
			new_conn_this_worker.ListenPort,
			(*upstream_all)[upstream_i],
			new_conn_this_worker.CliFd,
			upstream_fd,
			new_conn_this_worker.SA.(*unix.SockaddrInet4).Addr,
		)
		connection_read_status := ConnectionReadStatus{ReadCliEOF: false, ReadUpstreamEOF: false}

		mutex.Lock()
		(*connCli2UpstreamInfo)[new_conn_this_worker.CliFd] = &ConnectionFdReadStatus{Fd: upstream_fd,
			ConnectionReadStatus: connection_read_status,
			RequestId:            reqId,
			ListenFd:             listen_fd,
			Upstream:             (*upstream_all)[upstream_i],
			SA:                   new_conn_this_worker.SA,
		}
		(*connUpstream2CliInfo)[upstream_fd] = &ConnectionFdReadStatus{Fd: new_conn_this_worker.CliFd,
			ConnectionReadStatus: connection_read_status,
			RequestId:            reqId,
			ListenFd:             listen_fd,
			Upstream:             (*upstream_all)[upstream_i],
			SA:                   new_conn_this_worker.SA,
		}
		mutex.Unlock()

		AddEpoll(epfd, upstream_fd)
		AddEpoll(epfd, new_conn_this_worker.CliFd)
	}
}

func ConnectUpstream(upstream IpPort) (upstream_fd int) {
	upstream_fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		log.Fatal("my unix.Socket:", err)
	}
	err = unix.Connect(upstream_fd, &unix.SockaddrInet4{
		Port: upstream.Port,
		Addr: upstream.Ip,
	})
	if err != nil {
		log.Fatal("my unix.Connect:", upstream.Port, upstream.Ip, "with err:", err)
	}
	return
}

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

func main() {
	tcp_listener_upstream_all := getListenerUpstream()
	InitWorkerInfoAll()
	StartWorker()
	StartTCPProxyListen(tcp_listener_upstream_all)
}

/*
idea:
	动态加载监听端口
	处理连接到上游异常的情况
	处理客户端rst的情况

优化：
	考虑哪些场景使用interface

*/
