package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

type FD int
type ConnectionReadStatus struct {
	Fd      FD
	ReadEOF bool
}

type ConnectionInfo struct {
	ListenFd            FD
	ListenPort          int
	SA                  *unix.Sockaddr
	Upstream            *UpStream
	RequestId           string
	CliFd               FD
	UpstreamFd          FD
	CliInDataBytes      int
	UpstreamInDataBytes int
	StartTime           time.Time
}

type ConnectionInfoStatus struct {
	*ConnectionInfo
	ConnectionReadStatus
}

type NewConnectionInfo struct {
	CliFd FD
	*ConnectionInfo
}

type IpPort struct {
	Ip   [4]byte
	Port int
}

type UpStream struct {
	IpPort
	IsHealthy         bool
	HealthyUpdateTime time.Time
}
type UpStreamSlice []UpStream

type TcpListenIpPortUpstream struct {
	IpPortListen IpPort
	Upstream     *UpStreamSlice
}

type TcpListenIpPortUpstreamSlice []TcpListenIpPortUpstream

type Worker struct {
	EpFd                 FD // epoll使用的fd，client或者upstream来数据时的事件
	events               []unix.EpollEvent
	buf                  []byte
	pbuf                 []byte
	goroutineIndex       int
	UpstreamReqCount     map[FD]int
	UpstreamCurConnCount []int
}

// fd对应的ConnectionInfoStatus，fd转换为int
type FdConnectionInfoStatus []*ConnectionInfoStatus

// 为了降低内存占用，使用二维slice存储。如果阶梯为10，则1存储为[0][1], 11存储为[1][1]
type AllFdConnectionInfoStatus []*FdConnectionInfoStatus

const LISTENQUEUECOUNT = 12345
const WORKERCOUNT = 8
const RECV_SIZE = 4096
const UpstreamUnhealthyTimeOut = 300
const StageCount = 10000

var (
	WorkerAll                          [WORKERCOUNT]Worker
	Upstream_index                     map[IpPort]int = make(map[IpPort]int, LISTENQUEUECOUNT)
	mutex_Upstream_index               sync.RWMutex
	NewConnectionAll                   chan NewConnectionInfo          = make(chan NewConnectionInfo, LISTENQUEUECOUNT)
	LastTcpListenIpPortUpstreamAll     TcpListenIpPortUpstreamSlice    = make(TcpListenIpPortUpstreamSlice, 0)    // 当前使用的监听和对应的上游配置
	ListenFd_2_TcpListenIpPortUpstream map[FD]*TcpListenIpPortUpstream = make(map[FD]*TcpListenIpPortUpstream, 0) // 监听fd和监听端口、upstream的映射
	AllConnectionInfoStatus            AllFdConnectionInfoStatus       = make(AllFdConnectionInfoStatus, StageCount)
)

// 分割线

func (epfd FD) add_epoll(fd FD) {
	err := unix.SetNonblock(int(fd), true)
	if err != nil {
		fmt.Println("unix.SetNonblock:", err)
	}
	err = unix.EpollCtl(int(epfd), unix.EPOLL_CTL_ADD, int(fd), &unix.EpollEvent{
		Events: unix.EPOLLIN | unix.EPOLLET,
		Fd:     int32(fd),
	})
	if err != nil {
		log.Fatal("AddEpoll error:", err)
	}
}

func (epfd FD) del_epoll(fd FD) {
	err := unix.EpollCtl(int(epfd), unix.EPOLL_CTL_DEL, int(fd), &unix.EpollEvent{
		Events: unix.EPOLLIN | unix.EPOLLET,
		Fd:     int32(fd),
	})
	if err != nil {
		log.Println("DelEpoll error:", err)
	}
}

func (fd FD) close() {
	err := unix.Close(int(fd))
	if err != nil {
		fmt.Printf("close fd %d err, msg: %v", fd, err)
	}
}

func (fd FD) write(data *[]byte) (err error) {
	_, err = unix.Write(int(fd), *data)
	return
}

func (cli_fd FD) fin_connect_upstream_err() {
	err_msg := []byte("connect upstream error")
	cli_fd.write(&err_msg)
	cli_fd.close()
}

func (listen_fd FD) get_upstreamslice_by_listen_fd() (upstream *UpStreamSlice) {
	upstream = ListenFd_2_TcpListenIpPortUpstream[listen_fd].Upstream
	return
}

func (listen_epfd FD) loop_listen() {
	events_listen := make([]unix.EpollEvent, 1024)
	for {
		nevents, err := unix.EpollWait(int(listen_epfd), events_listen, -1)

		if err != nil {
			// 重启中断的系统调用
			if err.Error() == "interrupted system call" {
				continue
			} else {
				log.Fatal("my unix.EpollWait: ", err)
			}
		}

		for ev := 0; ev < nevents; ev++ {
			listen_fd := FD(events_listen[ev].Fd)
			ListenSock, err := unix.Getsockname(int(listen_fd))
			if err != nil {
				log.Fatal("error, unix.Getsockname(listen_fd): ", err)
			}
			for {
				cli_fd, sa, err := unix.Accept(int(listen_fd))
				if err != nil {
					if err == unix.EAGAIN {
						break
					} else {
						fmt.Println("unix.Accept error:", err)
						continue
					}
				}
				NewConnectionAll <- NewConnectionInfo{
					CliFd: FD(cli_fd),
					ConnectionInfo: &ConnectionInfo{
						ListenFd:   listen_fd,
						SA:         &sa,
						ListenPort: ListenSock.(*unix.SockaddrInet4).Port,
						StartTime:  time.Now(),
					},
				}
			}
		}
	}
}

func (fd FD) set_ConnectionInfoStatus(connection_info_status *ConnectionInfoStatus) {
	fd_i := int(fd)
	j := fd_i / StageCount
	k := fd_i % StageCount

	if AllConnectionInfoStatus[j] == nil {
		new_fd_connection_info_status := make(FdConnectionInfoStatus, StageCount)
		AllConnectionInfoStatus[j] = &new_fd_connection_info_status
	}
	(*AllConnectionInfoStatus[j])[k] = connection_info_status
}

func (fd FD) get_ConnectionInfoStatus() (connection_info_status *ConnectionInfoStatus) {
	fd_i := int(fd)
	j := fd_i / StageCount
	k := fd_i % StageCount
	connection_info_status = (*AllConnectionInfoStatus[j])[k]
	return
}

func (fd FD) delete_ConnectionInfoStatus() {
	fd_i := int(fd)
	j := fd_i / StageCount
	k := fd_i % StageCount

	(*AllConnectionInfoStatus[j])[k] = nil
}

func (ip_port IpPort) connect() (fd FD, err error) {
	upstream_fd_i, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		log.Fatal("my unix.Socket:", err)
	}
	err = unix.Connect(upstream_fd_i, &unix.SockaddrInet4{
		Port: ip_port.Port,
		Addr: ip_port.Ip,
	})

	if err != nil {
		log.Println("my unix.Connect:", ip_port.Port, ip_port.Ip, "with err:", err)
		// log.Error()
	}

	fd = FD(upstream_fd_i)
	return
}

func (ip_port IpPort) get_upstream_index() (index int) {
	mutex_Upstream_index.RLock()
	index = Upstream_index[ip_port]
	mutex_Upstream_index.RUnlock()
	return
}

func (ip_port IpPort) new_listen() (listen_fd FD) {
	listen_fd_i, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		log.Fatal(err)
	}
	err = unix.Bind(listen_fd_i, &unix.SockaddrInet4{
		Port: ip_port.Port,
		Addr: ip_port.Ip,
	})
	if err != nil {
		log.Fatal(err)
	}
	err = unix.Listen(listen_fd_i, 1024)
	if err != nil {
		log.Fatal(err)
	}
	listen_fd = FD(listen_fd_i)
	return
}

func (workerinfo *Worker) fin_request(event_in_fd, event_out_fd FD) {
	connection_info := event_in_fd.get_ConnectionInfoStatus().ConnectionInfo
	upstream := connection_info.Upstream
	duration := time.Now().Sub(connection_info.StartTime).Milliseconds()
	fmt.Printf("%s FIN, reqId: %s, in_bytes: %d, out_bytes: %d, duration: %dms\n",
		time.Now().Format("2006-01-02_15:04:05"),
		connection_info.RequestId,
		connection_info.CliInDataBytes,
		connection_info.UpstreamInDataBytes,
		duration,
	)
	event_out_fd.delete_ConnectionInfoStatus()
	event_in_fd.delete_ConnectionInfoStatus()
	event_out_fd.close()
	event_in_fd.close()

	index := upstream.IpPort.get_upstream_index()
	workerinfo.UpstreamCurConnCount[index]--
}

func (workerinfo *Worker) handle_error_terminate(event_in_fd, event_out_fd FD) {
	epfd := workerinfo.EpFd
	epfd.del_epoll(event_in_fd)
	epfd.del_epoll(event_out_fd)
	// 可能有问题，event_in_fd可能在此之前比其他协程使用
	workerinfo.fin_request(event_in_fd, event_out_fd)
}

func (workerinfo *Worker) handle_recv_data(event_in_fd FD) {
	buf := &(workerinfo.buf)
	pbuf := &(workerinfo.pbuf)
	epfd := workerinfo.EpFd

	fdOutInfo := event_in_fd.get_ConnectionInfoStatus()
	event_out_fd := fdOutInfo.Fd
	fdInInfo := event_out_fd.get_ConnectionInfoStatus()

	for {
		n, err := unix.Read(int(event_in_fd), *buf)

		need_break := true
		// Error
		switch err {
		case nil:
			need_break = false
		case unix.EBADFD:
			// 可能有问题，event_in_fd可能在此之前比其他协程使用
			workerinfo.handle_error_terminate(event_in_fd, event_out_fd)
		case unix.EAGAIN:
			// do nothing
		case unix.ECONNRESET:
			// 可能有问题，event_in_fd可能在此之前比其他协程使用
			workerinfo.handle_error_terminate(event_in_fd, event_out_fd)
		default:
			fmt.Println("encounter error:", err)
			workerinfo.handle_error_terminate(event_in_fd, event_out_fd)
		}

		if need_break {
			break
		}

		connection_info := event_in_fd.get_ConnectionInfoStatus()
		// debug
		fmt.Printf("debug: reqId: %s, read_n: %d\n", connection_info.RequestId, n)

		// EOF
		if n == 0 {
			unix.Shutdown(int(event_out_fd), unix.SHUT_WR)
			epfd.del_epoll(event_in_fd)
			fdInInfo.ReadEOF = true
			is_other_side_eof := fdOutInfo.ReadEOF

			if is_other_side_eof {
				workerinfo.fin_request(event_in_fd, event_out_fd)
			}

			break
		} else if n > 0 {
			// read ok
			*pbuf = (*buf)[0:n]

			connection_info := event_in_fd.get_ConnectionInfoStatus()
			// debug
			// fmt.Printf("debug: reqId: %s, read_n: %d\n", connection_info.RequestId, n)

			switch event_in_fd {
			case connection_info.CliFd:
				connection_info.CliInDataBytes += n
			case connection_info.UpstreamFd:
				connection_info.UpstreamInDataBytes += n
			default:
				fmt.Println("my debug, event_in_fd:", event_in_fd, "connection_info.CliFd:", connection_info.CliFd, "connection_info.UpstreamFd:", connection_info.UpstreamFd)
			}

			err := event_out_fd.write(pbuf)

			if err != nil {
				fmt.Println("write error:", err)
			}
		}
	}
}

func (workerinfo *Worker) worker_handle_events() {
	epfd := int(workerinfo.EpFd)
	events := &(workerinfo.events)

	for {
		nevents, err := unix.EpollWait(epfd, *events, -1)

		if err != nil {
			// 重启中断的系统调用
			if err == unix.EINTR {
				continue
			} else {
				log.Fatal("my unix.EpollWait: ", err)
			}
		}

		for ev := 0; ev < nevents; ev++ {
			event_fd := (*events)[ev].Fd
			// 处理单次读事件
			workerinfo.handle_recv_data(FD(event_fd))
		}
	}
}

func (workerinfo *Worker) get_upstream_rr(new_conn_this_worker NewConnectionInfo) (upstream *UpStream) {
	upstream_all := new_conn_this_worker.ListenFd.get_upstreamslice_by_listen_fd()
	upstream_all_healthy := make([]*UpStream, 0)
	listen_fd := new_conn_this_worker.ListenFd
	upstream_all_req_count := workerinfo.UpstreamReqCount
	upstream_all_count, ok := upstream_all_req_count[listen_fd]
	if !ok {
		upstream_all_req_count[listen_fd] = 0
		upstream_all_count = 0
	}

	for _, upstream_temp := range *upstream_all {
		if upstream_temp.IsHealthy {
			upstream_all_healthy = append(upstream_all_healthy, &upstream_temp)
		}
	}

	// upstream全部异常，尝试使用所有的
	if len(upstream_all_healthy) == 0 {
		for _, upstream_temp := range *upstream_all {
			upstream_all_healthy = append(upstream_all_healthy, &upstream_temp)
		}
	}

	upstream_i := upstream_all_count % len(upstream_all_healthy)
	upstream = (upstream_all_healthy)[upstream_i]
	upstream_all_req_count[listen_fd]++
	return
}

func (workerinfo *Worker) worker_handle_new_conn() {
	epfd := workerinfo.EpFd

	for new_conn_this_worker := range NewConnectionAll {
		// 负载均衡为轮询
		listen_fd := new_conn_this_worker.ListenFd
		var upstream *UpStream
		var upstream_fd FD
		var err error
		var upstream_all = ListenFd_2_TcpListenIpPortUpstream[listen_fd].Upstream
		var connect_upstream_max_retries = len(*upstream_all)

		// 更新upstream健康状态
		UpdateUpstreamHealth(upstream_all)

		for m := 0; m < connect_upstream_max_retries; m++ {
			upstream = workerinfo.get_upstream_rr(new_conn_this_worker)
			upstream_fd, err = upstream.connect()
			if err == nil {
				index := upstream.IpPort.get_upstream_index()
				workerinfo.UpstreamCurConnCount[index]++
				break
			} else {
				for n, upstream_temp := range *upstream_all {
					if upstream_temp == *upstream {
						(*upstream_all)[n].IsHealthy = false
						(*upstream_all)[n].HealthyUpdateTime = time.Now()
						break
					}
				}
			}
		}

		if err != nil {
			new_conn_this_worker.CliFd.fin_connect_upstream_err()
			break
		}

		reqId := fmt.Sprintf("%d-%d", workerinfo.goroutineIndex, workerinfo.UpstreamReqCount[listen_fd])

		fmt.Printf("%s goroutine: %d, NEW reqId: %s, ListenFd: %d, ListenPort: %d, Upstream: %v, %d -> srv -> %d, client: %v, connectioninfo: %p\n",
			time.Now().Format("2006-01-02_15:04:05"),
			workerinfo.goroutineIndex,
			reqId,
			listen_fd,
			new_conn_this_worker.ListenPort,
			upstream,
			new_conn_this_worker.CliFd,
			upstream_fd,
			(*new_conn_this_worker.SA).(*unix.SockaddrInet4).Addr,
			new_conn_this_worker.ConnectionInfo,
		)

		var connCliInfo, connUpstreamInfo ConnectionInfoStatus

		new_conn_this_worker.ConnectionInfo.CliFd = new_conn_this_worker.CliFd
		new_conn_this_worker.ConnectionInfo.UpstreamFd = upstream_fd

		connCliInfo = ConnectionInfoStatus{
			ConnectionInfo: new_conn_this_worker.ConnectionInfo,
			ConnectionReadStatus: ConnectionReadStatus{
				Fd:      upstream_fd,
				ReadEOF: false,
			},
		}
		connCliInfo.ConnectionInfo.RequestId = reqId
		connCliInfo.ConnectionInfo.Upstream = upstream
		connUpstreamInfo = ConnectionInfoStatus{
			ConnectionInfo: new_conn_this_worker.ConnectionInfo,
			ConnectionReadStatus: ConnectionReadStatus{
				Fd:      new_conn_this_worker.CliFd,
				ReadEOF: false,
			},
		}
		connUpstreamInfo.ConnectionInfo.RequestId = reqId
		connUpstreamInfo.ConnectionInfo.Upstream = upstream

		new_conn_this_worker.CliFd.set_ConnectionInfoStatus(&connCliInfo)
		upstream_fd.set_ConnectionInfoStatus(&connUpstreamInfo)

		epfd.add_epoll(upstream_fd)
		epfd.add_epoll(new_conn_this_worker.CliFd)
	}
}

func (tcp_listener_upstream_all TcpListenIpPortUpstreamSlice) new_listen_all(epfd_listen FD) {
	for k, _ := range tcp_listener_upstream_all {
		listen_fd := tcp_listener_upstream_all[k].IpPortListen.new_listen()
		epfd_listen.add_epoll(listen_fd)
		ListenFd_2_TcpListenIpPortUpstream[listen_fd] = &(tcp_listener_upstream_all[k])
	}
}

func (tcp_listener_upstream_all TcpListenIpPortUpstreamSlice) delete_listen_all(epfd_listen FD) {
	for _, tcp_listener_upstream := range tcp_listener_upstream_all {
		for listenfd, listenfd_tcp_listener_upstream := range ListenFd_2_TcpListenIpPortUpstream {
			if tcp_listener_upstream.IpPortListen == listenfd_tcp_listener_upstream.IpPortListen {
				epfd_listen.del_epoll(listenfd)
				listenfd.close()
				delete(ListenFd_2_TcpListenIpPortUpstream, listenfd)
				break
			}
		}
	}
}

func (tcp_listener_upstream_all TcpListenIpPortUpstreamSlice) load_listen(listen_epfd FD) {
	tcp_listener_upstream_all_add, tcp_listener_upstream_all_del := LastTcpListenIpPortUpstreamAll.compare2tcp_listener_upstream_all(tcp_listener_upstream_all)
	tcp_listener_upstream_all_del.delete_listen_all(listen_epfd)
	LastTcpListenIpPortUpstreamAll = tcp_listener_upstream_all
	tcp_listener_upstream_all_add.new_listen_all(listen_epfd)
}

func (tcp_listener_upstream_all TcpListenIpPortUpstreamSlice) gen_upstream_index() {
	index := 0
	for _, tcp_listener_upstream := range tcp_listener_upstream_all {
		for _, upstream := range *tcp_listener_upstream.Upstream {
			mutex_Upstream_index.Lock()
			Upstream_index[upstream.IpPort] = index
			mutex_Upstream_index.Unlock()
			index++
		}
	}
}

func (old_tcp_listener_upstream_all TcpListenIpPortUpstreamSlice) compare2tcp_listener_upstream_all(new_tcp_listener_upstream_all TcpListenIpPortUpstreamSlice) (
	set_add, set_del TcpListenIpPortUpstreamSlice) {
	for _, old_tcp_listener_upstream := range old_tcp_listener_upstream_all {
		found_listener := false
		for _, new_tcp_listener_upstream := range new_tcp_listener_upstream_all {
			if old_tcp_listener_upstream.IpPortListen == new_tcp_listener_upstream.IpPortListen {
				found_listener = true
				for listen_fd, listen_fd_tcp_listener_upstream := range ListenFd_2_TcpListenIpPortUpstream {
					if new_tcp_listener_upstream.IpPortListen == listen_fd_tcp_listener_upstream.IpPortListen {
						fmt.Println("updating upstream:", ListenFd_2_TcpListenIpPortUpstream[listen_fd].Upstream, " -> ", new_tcp_listener_upstream.Upstream)
						ListenFd_2_TcpListenIpPortUpstream[listen_fd].Upstream = new_tcp_listener_upstream.Upstream
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
			if old_tcp_listener_upstream.IpPortListen == new_tcp_listener_upstream.IpPortListen {
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

func new_listen_efd() (listen_epfd FD) {
	listen_epfd_int, err := unix.EpollCreate1(0)
	if err != nil {
		log.Fatal(err)
	}
	listen_epfd = FD(listen_epfd_int)
	return
}

func StartWorker() {
	for i := 0; i < WORKERCOUNT; i++ {
		go (&WorkerAll[i]).worker_handle_events()
		go (&WorkerAll[i]).worker_handle_new_conn()
	}
}

func UpdateUpstreamHealth(UpstreamAll *UpStreamSlice) {
	time_now := time.Now()
	for i := 0; i < len(*UpstreamAll); i++ {
		if !(*UpstreamAll)[i].IsHealthy && time_now.Sub((*UpstreamAll)[i].HealthyUpdateTime) > (UpstreamUnhealthyTimeOut*time.Second) {
			(*UpstreamAll)[i].IsHealthy = true
			(*UpstreamAll)[i].HealthyUpdateTime = time_now
		}
	}
}

func InitWorkerInfoAll() {
	for i := 0; i < WORKERCOUNT; i++ {
		epfd, err := unix.EpollCreate1(0)
		if err != nil {
			log.Fatal(err)
		}
		worker_info := &(WorkerAll[i])
		worker_info.EpFd = FD(epfd)
		worker_info.events = make([]unix.EpollEvent, LISTENQUEUECOUNT)
		worker_info.buf = make([]byte, RECV_SIZE)
		worker_info.pbuf = make([]byte, RECV_SIZE)
		worker_info.goroutineIndex = i
		worker_info.UpstreamReqCount = make(map[FD]int, 0)
		// TODO for test
		worker_info.UpstreamCurConnCount = make([]int, LISTENQUEUECOUNT)
	}
}

func main() {
	// for test
	my_temp_test()

	efd_listen := new_listen_efd()
	InitWorkerInfoAll()
	StartWorker()
	tcp_listener_upstream_all := getListenerUpstream()
	tcp_listener_upstream_all.gen_upstream_index()
	tcp_listener_upstream_all.load_listen(efd_listen)

	// 仅用于测试动态加载配置
	// go func() {
	// 	tcp_listener_upstream_all.load_listen(efd_listen)
	// 	new_tcp_listener_upstream_all := getListenerUpstreamNew()
	// 	fmt.Println("now sleep 300s")
	// 	time.Sleep(300 * time.Second)
	// 	fmt.Println("now wake up")
	// 	new_tcp_listener_upstream_all.load_listen(efd_listen)
	// }()

	// go printGoroutineInfo()

	efd_listen.loop_listen()
}

/*
idea:
	连接到上游，支持多个负载均衡策略
	每个连接，记录可读事件状态，每次读取部分数据
	支持tls
	暴露prometheus exporter
	日志
		打印到文件，同时支持打印到标准输出，日志分级
		重新考虑是否结束时打印日志
优化：
	考虑哪些场景使用interface


Done:
	动态加载监听端口
	日志增加时间
	处理客户端rst的情况
	处理连接到上游异常的情况
		增加超时重置异常为正常
	日志
		打印处理时长
		打印发送和接收字节数
*/
