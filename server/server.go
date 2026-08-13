package server

import (
	"context"
	"github.com/DeleteElf/zero-net/framework"
	"github.com/DeleteElf/zero-net/framework/streams"
	"github.com/DeleteElf/zero-net/framework/utils"
	"github.com/DeleteElf/zero-net/stunhelper"
	"github.com/quic-go/quic-go"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const MaxStreamCount = 6

type Stream struct {
	Info   *streams.StreamInfo
	Stream *quic.Stream
	Server *Server
}

type Server struct {
	//stun服务地址
	Stun string
	//外网地址
	ExternalIp   string
	ExternalPort int

	isAgent  bool
	NetConn  net.PacketConn
	listener *quic.Listener
	Sockets  map[string]*streams.Socket

	QuicConfig *quic.Config
	lock       sync.Mutex

	OnAcceptSocket       streams.SocketCallbackFunc
	OnSocketDisConnected streams.SocketCallbackFunc

	framework.CloseableObject
}

// NewServerByAddress 创建新的服务实例，根据设置的地址监听
func NewServerByAddress(address string) *Server {
	netConn, err := streams.NewUdpSocketServer(address)
	if err != nil {
		slog.Error("创建socket服务失败！", slog.Any("err", err))
		return nil
	}
	return NewServer(netConn, false)
}

// NewServerByPort 创建新的服务实例，并默认监听 0.0.0.0
func NewServerByPort(port int) *Server {
	return NewServerByAddress("0.0.0.0:" + strconv.Itoa(port))
}

func NewAgentServer(address string) *Server {
	tlsConfig := utils.GenTLSConfig()
	quicConfig := &quic.Config{
		// MaxIncomingStreams: 0xffffffffffff, // 最大默认stream输入，默认100
		HandshakeIdleTimeout:    5 * time.Second,  // 默认5s
		MaxIdleTimeout:          30 * time.Second, // 默认30s
		KeepAlivePeriod:         3 * time.Second,  // 建议是 MaxIdleTimeout 的一半，或者更小的值
		InitialPacketSize:       1500,             //初始包大小
		DisablePathMTUDiscovery: true,
		Allow0RTT:               true,
	}
	listener, err := quic.ListenAddr(address, tlsConfig, quicConfig) //这种写法，拿不到conn
	if err != nil {
		slog.Error("创建socket服务失败！", slog.Any("err", err))
		return nil
	}
	svr := &Server{
		isAgent: true,
		//NetConn: conn,
		listener: listener,
		Sockets:  make(map[string]*streams.Socket),
	}
	svr.IsClosed = false
	svr.SetOnCloseHandler(svr)
	return svr
}

// NewServer 创建新的服务实例
func NewServer(conn net.PacketConn, isAgent bool) *Server {
	svr := &Server{
		isAgent: isAgent,
		NetConn: conn,
		Sockets: make(map[string]*streams.Socket),
	}
	svr.IsClosed = false
	svr.SetOnCloseHandler(svr)
	return svr
}

func (s *Server) OnClosing() bool {
	slog.Debug("正在关闭服务端")
	// 服务端先关闭，避免监听无法关闭
	if s.NetConn != nil {
		_ = s.NetConn.Close()
		s.NetConn = nil
	}
	if s.listener != nil {
		_ = s.listener.Close()
		s.listener = nil
	}
	for key, _ := range s.Sockets {
		_ = s.CloseSocket(key)
	}
	s.Sockets = nil
	return true
}

func (s *Server) OnClosed() {
	slog.Debug("服务端已经关闭")
}

func (s *Server) DetectStun(token string) {
	if len(s.Stun) > 0 {
		slog.Debug("配置了stun服务，正在准备探测！", slog.String("address", s.Stun))
		cli := stunhelper.NewClient()
		err := cli.Connect(s.Stun, token, s.NetConn)
		if err == nil {
			slog.Debug("你的公网 IP 地址 :", slog.Any("ip", cli.ExternalAddress.IP))
			slog.Debug("你的公网映射端口 : ", slog.Int("port", cli.ExternalAddress.Port))
			s.ExternalIp = cli.ExternalAddress.IP.String()
			s.ExternalPort = cli.ExternalAddress.Port
		}
		//cli.Close()
	}
}

func (s *Server) StartListen(onDisconnect streams.SocketCallbackFunc) {
	tlsConfig := utils.GenTLSConfig()
	s.QuicConfig = &quic.Config{
		// MaxIncomingStreams: 0xffffffffffff, // 最大默认stream输入，默认100
		HandshakeIdleTimeout:    5 * time.Second,  // 默认5s
		MaxIdleTimeout:          10 * time.Second, // 默认30s
		KeepAlivePeriod:         3 * time.Second,  // 建议是 MaxIdleTimeout 的一半，或者更小的值
		InitialPacketSize:       1500,             //初始包大小
		DisablePathMTUDiscovery: false,            // 允许路径 MTU 探索
		Allow0RTT:               true,
		EnableDatagrams:         true, //允许直接传输udp
	}
	// 4. 构建 quic.Transport（复用刚刚创建的底层 conn）
	tr := &quic.Transport{
		Conn: s.NetConn,
	}
	var err error
	s.listener, err = tr.Listen(tlsConfig, s.QuicConfig)

	//s.listener, err = quic.Listen(s.NetConn, tlsConfig, quicConfig)
	if err != nil {
		slog.Error("启动服务监听发生错误！", slog.Any("err", err))
		return
	}
	slog.Info("服务启动监听", slog.Any("addr", s.NetConn.LocalAddr()))
	for {
		if s.IsClosed { //已经关闭则退出
			break
		}
		if s.listener == nil {
			break
		}
		quicConn, err := s.listener.Accept(context.TODO())
		if s.IsClosed { //不再接受新的连接
			break
		}
		if err != nil {
			slog.Warn("接入连接失败", slog.Any("err", err))
			break
		}
		slog.Info("接入一个新的连接", slog.Any("addr", quicConn.RemoteAddr()))
		go s.acceptConnection(quicConn, onDisconnect)
	}
	slog.Info("服务停止监听")
}

func (s *Server) acceptConnection(quicConn *quic.Conn, onDisconnect streams.SocketCallbackFunc) {
	defer func() {
		slog.Info("连接断开", slog.Any("addr", quicConn.RemoteAddr()))
		_ = quicConn.CloseWithError(0, "other")
	}()

	for {
		if s.IsClosed {
			break
		}
		stream, err := quicConn.AcceptStream(context.TODO())
		if err != nil {
			if !strings.HasPrefix(err.Error(), "Application error 0x0 (remote)") {
				slog.Error("接入一个新的流发生错误", slog.Any("err", err))
			}
			return
		}
		go s.processStream(quicConn, stream, onDisconnect)
	}
}

func (s *Server) processStream(quicConn *quic.Conn, stream *quic.Stream, onDisconnect streams.SocketCallbackFunc) {
	streamId := stream.StreamID()
	info, err := streams.ReadStreamInfo(stream)
	if err != nil {
		slog.Error("获取流信息失败", slog.Any("streamId", streamId), slog.Any("err", err))
		_ = streams.CloseStream(stream)
		return
	}
	if err := streams.ValidateStreamInfo(info); err != nil {
		slog.Warn("无效的流信息", slog.Any("err", err))
		_ = streams.CloseStream(stream)
		return
	}
	if info.Index < 0 || info.Index >= MaxStreamCount {
		slog.Error("无效的通道", slog.Int("chn", info.Index))
		_ = streams.CloseStream(stream)
		return
	}
	slog.Info("启动通道通讯", slog.Int("chn", info.Index), slog.Any("streamId", streamId), slog.String("clientId", info.Id))
	s.lock.Lock()
	if s.Sockets[info.Id] == nil {
		socket := streams.NewSocket(info.Id, info.Count, func(sock *streams.Socket) {
			if s.Sockets[sock.Id] != nil {
				s.Sockets[sock.Id] = nil
				delete(s.Sockets, sock.Id)
			}
			if onDisconnect != nil {
				onDisconnect(sock)
			}
		})
		socket.Conn = quicConn
		s.Sockets[info.Id] = socket
		if s.OnAcceptSocket != nil {
			s.OnAcceptSocket(socket)
		}
		if s.QuicConfig.EnableDatagrams {
			if socket.PacketPool == nil {
				socket.PacketPool = socket.CreatePacketPool(s.QuicConfig.InitialPacketSize)
			}
			go socket.HandleChannelStreamDatagram()
		}
	}
	if s.QuicConfig.EnableDatagrams && s.Sockets[info.Id].StreamChannels[info.Index].Encoder == nil {
		s.Sockets[info.Id].SetFecParam(info.Index, info.DataShards, info.ParityShards)
	}
	s.lock.Unlock()
	socket := s.Sockets[info.Id]
	go socket.HandleChannelStreamData(info.Index, stream)
}
func (s *Server) CloseSocket(id string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.Sockets[id] != nil {
		s.Sockets[id].Close()
		s.Sockets[id] = nil
		delete(s.Sockets, id)
	}
	return nil
}
func (s *Server) GetSocket(id string) *streams.Socket {
	if s.Sockets[id] != nil {
		return s.Sockets[id]
	}
	return nil
}
