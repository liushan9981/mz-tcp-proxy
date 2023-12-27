package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const LISTENQUEUECOUNT = 1024
const WORKERCOUNT = 8
const RECV_SIZE = 4096

type ConnectionReadStatus struct {
	RequestId  string
	ListenFd   int
	ListenPort int
	Upstream   IpPort
	Fd         int
	SA         unix.Sockaddr
	ReadEOF    bool
}

type WorkerInfo struct {
	EpFd                int
	connCliUpstreamInfo *(map[int]*ConnectionReadStatus)
	mutex               sync.RWMutex
	events              []unix.EpollEvent
	buf                 []byte
	pbuf                []byte
	goroutineIndex      int
	NewConnectionAll    chan NewConn
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
	WorkerInfoAll                  [WORKERCOUNT]WorkerInfo
	LastTcpListenerUpstreamAll     []TcpListenerUpstream        = make([]TcpListenerUpstream, 1)
	LISTENFD_2_TcpListenerUpstream map[int]*TcpListenerUpstream = make(map[int]*TcpListenerUpstream, 1)
)

func InitWorkerInfoAll() {
	for i := 0; i < WORKERCOUNT; i++ {
		epfd, err := unix.EpollCreate1(0)
		if err != nil {
			log.Fatal(err)
		}
		worker_info := &(WorkerInfoAll[i])
		worker_info.EpFd = epfd
		conn_cli_upstream_info := make(map[int]*ConnectionReadStatus, LISTENQUEUECOUNT)
		worker_info.connCliUpstreamInfo = &conn_cli_upstream_info
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

func new_listen_all(tcp_listener_upstream_all []TcpListenerUpstream, epfd_listen int) {
	for k, _ := range tcp_listener_upstream_all {
		listen_fd := new_listen(tcp_listener_upstream_all[k].Listener)
		AddEpoll(epfd_listen, listen_fd)
		LISTENFD_2_TcpListenerUpstream[listen_fd] = &(tcp_listener_upstream_all[k])
	}

}

func delete_listen_all(tcp_listener_upstream_all []TcpListenerUpstream, epfd_listen int) {
	for _, tcp_listener_upstream := range tcp_listener_upstream_all {
		for listenfd, listenfd_tcp_listener_upstream := range LISTENFD_2_TcpListenerUpstream {
			if tcp_listener_upstream.Listener == listenfd_tcp_listener_upstream.Listener {
				DelEpoll(epfd_listen, listenfd)
				unix.Close(listenfd)
				delete(LISTENFD_2_TcpListenerUpstream, listenfd)
				break
			}
		}
	}
}

func compare2tcp_listener_upstream_all(old_tcp_listener_upstream_all, new_tcp_listener_upstream_all []TcpListenerUpstream) (set_add, set_del []TcpListenerUpstream) {
	for _, old_tcp_listener_upstream := range old_tcp_listener_upstream_all {
		found_listener := false
		for _, new_tcp_listener_upstream := range new_tcp_listener_upstream_all {
			if old_tcp_listener_upstream.Listener == new_tcp_listener_upstream.Listener {
				found_listener = true
				for listen_fd, listen_fd_tcp_listener_upstream := range LISTENFD_2_TcpListenerUpstream {
					if new_tcp_listener_upstream.Listener == listen_fd_tcp_listener_upstream.Listener {
						fmt.Println("updating upstream:", LISTENFD_2_TcpListenerUpstream[listen_fd].Upstream, " -> ", new_tcp_listener_upstream.Upstream)
						LISTENFD_2_TcpListenerUpstream[listen_fd].Upstream = new_tcp_listener_upstream.Upstream
						break
					}
				}
				break
			}
		}
		if !found_listener {
			set_del = append(set_del, old_tcp_listener_upstream)
		}
	}

	for _, new_tcp_listener_upstream := range new_tcp_listener_upstream_all {
		found_listener := false
		for _, old_tcp_listener_upstream := range old_tcp_listener_upstream_all {
			if old_tcp_listener_upstream.Listener == new_tcp_listener_upstream.Listener {
				found_listener = true
				break
			}
		}

		if !found_listener {
			set_add = append(set_add, new_tcp_listener_upstream)
		}
	}

	return
}

func new_listen_efd() (listen_epfd int) {
	listen_epfd, err := unix.EpollCreate1(0)
	if err != nil {
		log.Fatal(err)
	}
	return
}

func StartTCPProxyListen(tcp_listener_upstream_all []TcpListenerUpstream, listen_epfd int) {
	tcp_listener_upstream_all_add, tcp_listener_upstream_all_del := compare2tcp_listener_upstream_all(LastTcpListenerUpstreamAll, tcp_listener_upstream_all)
	delete_listen_all(tcp_listener_upstream_all_del, listen_epfd)
	LastTcpListenerUpstreamAll = tcp_listener_upstream_all
	new_listen_all(tcp_listener_upstream_all_add, listen_epfd)
}

func StartTCPProxyLoopListen(listen_epfd int) {
	events_listen := make([]unix.EpollEvent, 1024)
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
					Upstream:   &(LISTENFD_2_TcpListenerUpstream[listen_fd].Upstream),
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

func FinRequest(conn_cli_upstream_info *map[int]*ConnectionReadStatus, event_in_fd, event_out_fd int, mutex *sync.RWMutex) {
	mutex.Lock()
	fmt.Printf("FIN, reqId: %s\n", (*conn_cli_upstream_info)[event_in_fd].RequestId)
	delete(*conn_cli_upstream_info, event_out_fd)
	delete(*conn_cli_upstream_info, event_in_fd)
	mutex.Unlock()

	unix.Close(event_out_fd)
	unix.Close(event_in_fd)
}

func handleInData(wokerinfo *WorkerInfo, event_in_fd int) {
	buf := &(wokerinfo.buf)
	pbuf := &(wokerinfo.pbuf)
	epfd := wokerinfo.EpFd
	mutex := &(wokerinfo.mutex)

	conn_cli_upstream_info := wokerinfo.connCliUpstreamInfo
	mutex.RLock()
	fdOutInfo := (*conn_cli_upstream_info)[event_in_fd]
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
				FinRequest(conn_cli_upstream_info, event_in_fd, event_out_fd, mutex)
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

			mutex.Lock()
			(*conn_cli_upstream_info)[event_out_fd].ReadEOF = true
			mutex.Unlock()

			if (*conn_cli_upstream_info)[event_in_fd].ReadEOF {
				FinRequest(conn_cli_upstream_info, event_in_fd, event_out_fd, mutex)
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
			// 处理单次读事件
			handleInData(wokerinfo, int(event_fd))
		}
	}
}

func HandleNewConnWorker(workerinfo *WorkerInfo) {
	mutex := &(workerinfo.mutex)
	connCliUpstreamInfo := workerinfo.connCliUpstreamInfo
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

		mutex.Lock()
		(*connCliUpstreamInfo)[new_conn_this_worker.CliFd] = &ConnectionReadStatus{Fd: upstream_fd,
			ReadEOF:   false,
			RequestId: reqId,
			ListenFd:  listen_fd,
			Upstream:  (*upstream_all)[upstream_i],
			SA:        new_conn_this_worker.SA,
		}
		(*connCliUpstreamInfo)[upstream_fd] = &ConnectionReadStatus{Fd: new_conn_this_worker.CliFd,
			ReadEOF:   false,
			RequestId: reqId,
			ListenFd:  listen_fd,
			Upstream:  (*upstream_all)[upstream_i],
			SA:        new_conn_this_worker.SA,
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

func main() {
	efd_listen := new_listen_efd()
	InitWorkerInfoAll()
	StartWorker()

	tcp_listener_upstream_all := getListenerUpstream()

	// 仅用于测试动态加载配置
	go func() {
		StartTCPProxyListen(tcp_listener_upstream_all, efd_listen)
		new_tcp_listener_upstream_all := getListenerUpstreamNew()
		fmt.Println("now sleep 300s")
		time.Sleep(300 * time.Second)
		fmt.Println("now wake up")
		StartTCPProxyListen(new_tcp_listener_upstream_all, efd_listen)
	}()

	StartTCPProxyLoopListen(efd_listen)
}

/*
idea:
	动态加载监听端口
	处理连接到上游异常的情况
	处理客户端rst的情况

优化：
	考虑哪些场景使用interface

*/
