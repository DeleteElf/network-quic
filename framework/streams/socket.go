package streams

import (
	"context"
	"errors"
	"github.com/DeleteElf/zero-net/framework"
	"github.com/DeleteElf/zero-net/framework/utils"
	"github.com/quic-go/quic-go"
	"log/slog"
	"net"
	"sync"
	"syscall"
)

// MessageCallbackFunc 消息事件回调
type MessageCallbackFunc func(string)

// SocketCallbackFunc socket事件回调
type SocketCallbackFunc func(*Socket)

func NewUdpSocketClient() (*net.UDPConn, error) {
	//svrAddr, err := net.ResolveUDPAddr(STREAM_NETWORK_UDP, serverAddr)
	//if err != nil {
	//	return nil, svrAddr, err
	//}
	conn, err := net.ListenUDP(STREAM_NETWORK_UDP, nil)
	if err != nil {
		return nil, err
	}
	err = conn.SetReadBuffer(DefaultBufferSize)
	if err != nil {
		return nil, err
	}
	err = conn.SetWriteBuffer(DefaultBufferSize)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func NewUdpSocketServer(addr string) (net.PacketConn, error) {
	config := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			err := c.Control(func(fd uintptr) {
				utils.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
			})
			return err
		},
	}
	config.SetMultipathTCP(false)
	conn, err := config.ListenPacket(context.Background(), STREAM_NETWORK_UDP, addr)

	if err != nil {
		return nil, err
	}
	if udpConn, ok := conn.(*net.UDPConn); ok {
		err = udpConn.SetReadBuffer(DefaultBufferSize)
		if err != nil {
			return nil, err
		}
		err = udpConn.SetWriteBuffer(DefaultBufferSize)
		if err != nil {
			return nil, err
		}
	}
	return conn, nil
}

// Socket 流基础对象
type Socket struct {
	Id             string
	StreamChannels []*StreamChannel
	ChannelCount   int

	OnDisconnect SocketCallbackFunc

	framework.CloseableObject
	StreamChannelOperating

	channelEditLock sync.Mutex
}

func NewSocket(id string, channelCount int, onDisconnect SocketCallbackFunc) *Socket {
	sock := &Socket{
		Id:           id,
		ChannelCount: channelCount,
	}
	sock.IsClosed = false
	sock.SetOnCloseHandler(sock)
	sock.OnDisconnect = onDisconnect
	sock.CreateChannels(channelCount)
	return sock
}
func (s *Socket) CloseChannel(channelIndex int) bool {
	s.channelEditLock.Lock()
	defer s.channelEditLock.Unlock()
	if len(s.StreamChannels) > channelIndex && s.StreamChannels[channelIndex] != nil {
		s.StreamChannels[channelIndex].Close()
		s.StreamChannels[channelIndex] = nil
		return true
	}
	return false
}

func (s *Socket) OnClosing() bool {
	s.channelEditLock.Lock()
	defer s.channelEditLock.Unlock()
	for i := 0; i < len(s.StreamChannels); i++ {
		if s.StreamChannels[i] != nil {
			s.StreamChannels[i].Close()
			s.StreamChannels[i] = nil
		}
	}
	s.ChannelCount = 0
	s.StreamChannels = make([]*StreamChannel, 0) //清空切片
	return true
}

func (s *Socket) OnClosed() {
	slog.Debug("socket 已经退出！", slog.String("id", s.Id))
	if s.OnDisconnect != nil {
		s.OnDisconnect(s)
	}
}

// CreateChannels 创建通道
func (s *Socket) CreateChannels(count int) {
	s.channelEditLock.Lock()
	defer s.channelEditLock.Unlock()
	s.StreamChannels = make([]*StreamChannel, count) //创建通道列表切片
	for i := 0; i < count; i++ {
		s.StreamChannels[i] = NewStreamChannel(s.Id, i) //make(chan StreamChannelData, 3) //创建通道实例
		s.StreamChannels[i].OnDisconnect = func(id string, index int) {
			if !s.IsClosed {
				s.channelEditLock.Lock()
				if index < len(s.StreamChannels) && s.StreamChannels[index] != nil {
					s.StreamChannels[index].Close()
					s.StreamChannels[index] = nil
				}
				s.channelEditLock.Unlock()
				if s.OnDisconnect != nil {
					finded := false
					for _, channel := range s.StreamChannels {
						if channel != nil && !channel.IsClosed {
							finded = true
							break
						}
					}
					if !finded {
						slog.Debug("socket的通道已全部断开连接！")
						s.Close()
					}
				}
			}
		}
	}
	s.ChannelCount = count
}

// HandleChannelStreamData 从通道接收流的数据
func (s *Socket) HandleChannelStreamData(channelId int, stream *quic.Stream) {
	s.StreamChannels[channelId].HandleChannelStreamData(stream)
}

func (s *Socket) ReceiveDataToBuffer(channelId int) (bool, error) {
	if len(s.StreamChannels) == 0 {
		return false, errors.New("当前socket的通道数为0！")
	}
	if channelId >= s.ChannelCount {
		return false, errors.New("超过通道允许范围！")
	}
	if s.StreamChannels[channelId] != nil {
		return s.StreamChannels[channelId].ReceiveDataToBuffer(), nil
	}
	return false, errors.New("通道未初始化！")
}

func (s *Socket) Send(channelId int, data []byte) (bool, error) {
	if s.IsClosed {
		return false, errors.New("socket is closed")
	}
	if channelId >= s.ChannelCount {
		return false, errors.New("超过通道允许范围！")
	}
	if len(s.StreamChannels) == 0 || s.StreamChannels[channelId] == nil {
		return false, errors.New("通道未初始化！")
	}
	return s.StreamChannels[channelId].Send(data)
}

func (s *Socket) Ping(channelId int) (bool, error) {
	if s.IsClosed {
		return false, nil
	}
	if channelId >= s.ChannelCount {
		return false, errors.New("超过通道允许范围！")
	}
	if s.StreamChannels[channelId] == nil {
		return false, errors.New("通道未初始化！")
	}
	ping := map[string]interface{}{
		"action": "ping",
		"from":   "host",
	}
	data, _ := utils.ToJsonByte(ping)
	return s.StreamChannels[channelId].Send(data)
}
