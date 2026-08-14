package server

import (
	"context"
	"github.com/DeleteElf/zero-net/framework"
	"github.com/DeleteElf/zero-net/framework/network"
	"github.com/DeleteElf/zero-net/framework/utils"
	"github.com/DeleteElf/zero-net/ice"
	"github.com/quic-go/quic-go"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"
)

const MaxStreamCount = 6

type Stream struct {
	Info   *network.StreamInfo
	Stream *quic.Stream
	Server *Server
}

type Server struct {
	////stun服务地址
	//Stun []string
	//外网地址
	ExternalIp   string
	ExternalPort int

	isAgent  bool
	listener *quic.Listener
	Sockets  map[string]*network.Socket

	Config *quic.Config
	lock   sync.Mutex

	OnAcceptSocket       network.SocketCallbackFunc
	OnSocketDisConnected network.SocketCallbackFunc
	ice.IceWorker
	framework.CloseableObject
}

// NewServerByAddress 创建新的服务实例，根据设置的地址监听
func NewServerByAddress(address string) *Server {
	netConn, err := network.NewUdpSocketServer(address)
	if err != nil {
		slog.Error("创建socket服务失败！", slog.Any("err", err))
		return nil
	}
	return NewServer(netConn, false)
}

// NewServer 创建新的服务实例
func NewServer(conn net.PacketConn, isAgent bool) *Server {
	svr := &Server{
		isAgent: isAgent,
		Sockets: make(map[string]*network.Socket),
	}
	svr.NetConn = conn
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

func (s *Server) StartListen(onDisconnect network.SocketCallbackFunc) {
	tlsConfig := utils.GenTLSConfig()
	if s.Config == nil {
		s.Config = &quic.Config{
			// MaxIncomingStreams: 0xffffffffffff, // 最大默认stream输入，默认100
			HandshakeIdleTimeout:    5 * time.Second,  // 默认5s
			MaxIdleTimeout:          10 * time.Second, // 默认30s
			KeepAlivePeriod:         3 * time.Second,  // 建议是 MaxIdleTimeout 的一半，或者更小的值
			InitialPacketSize:       1500,             //初始包大小
			DisablePathMTUDiscovery: false,            // 允许路径 MTU 探索
			Allow0RTT:               true,
			EnableDatagrams:         false, //允许直接传输udp
		}
	}
	// 4. 构建 quic.Transport（复用刚刚创建的底层 conn）
	tr := &quic.Transport{
		Conn: s.NetConn,
	}
	var err error
	s.listener, err = tr.Listen(tlsConfig, s.Config)

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
		//quicConn.ConnectionStats().SmoothedRTT //获取网络状态相关参数
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

func (s *Server) acceptConnection(quicConn *quic.Conn, onDisconnect network.SocketCallbackFunc) {
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

func (s *Server) processStream(quicConn *quic.Conn, stream *quic.Stream, onDisconnect network.SocketCallbackFunc) {
	streamId := stream.StreamID()
	info, err := network.ReadStreamInfo(stream)
	if err != nil {
		slog.Error("获取流信息失败", slog.Any("streamId", streamId), slog.Any("err", err))
		_ = network.CloseStream(stream)
		return
	}
	if err := network.ValidateStreamInfo(info); err != nil {
		slog.Warn("无效的流信息", slog.Any("err", err))
		_ = network.CloseStream(stream)
		return
	}
	if info.Index < 0 || info.Index >= MaxStreamCount {
		slog.Error("无效的通道", slog.Int("chn", info.Index))
		_ = network.CloseStream(stream)
		return
	}
	slog.Info("启动通道通讯", slog.Int("chn", info.Index), slog.Any("streamId", streamId), slog.String("clientId", info.Id))
	s.lock.Lock()
	if s.Sockets[info.Id] == nil {
		socket := network.NewSocket(info.Id, info.Count, func(sock *network.Socket) {
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
		if s.Config.EnableDatagrams {
			if socket.PacketPool == nil {
				socket.PacketPool = socket.CreatePacketPool(s.Config.InitialPacketSize)
			}
			go socket.HandleChannelStreamDatagram()
		}
	}
	if s.Config.EnableDatagrams && s.Sockets[info.Id].StreamChannels[info.Index].Encoder == nil {
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
func (s *Server) GetSocket(id string) *network.Socket {
	if s.Sockets[id] != nil {
		return s.Sockets[id]
	}
	return nil
}
