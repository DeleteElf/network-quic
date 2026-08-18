package network

import (
	"context"
	"encoding/binary"
	"errors"
	"github.com/DeleteElf/zero-net/framework"
	"github.com/DeleteElf/zero-net/framework/utils"
	"github.com/klauspost/reedsolomon"
	"github.com/quic-go/quic-go"
	"log/slog"
	"net"
	"sync"
	"syscall"
)

// FECPacket Fec数据包,包头长度 12字节
type FECPacket struct {
	//通道的编号，主要用于路由分发，1个字节
	ChannelId int
	//分组的编号，主要用于重组，8个字节，考虑到可能会播放很久,GroupId并不等于FrameIndex，一个数据包可能被拆成多个分组
	GroupId uint64
	//shard的索引 1个字节
	ShardIdx int
	//原始数据长度 2个字节 暂时不知道有没意义。。。
	//DataLength uint16
	DataShards   int
	ParityShards int
	Total        int
	//数据包体长度 2个字节
	Length  uint16
	Payload []byte
}

// MessageCallbackFunc 消息事件回调
type MessageCallbackFunc func(string)

// SocketCallbackFunc socket事件回调
type SocketCallbackFunc func(*Socket)

func NewUdpSocketClient() (*net.UDPConn, error) {
	conn, err := net.ListenUDP(STREAM_NETWORK_UDP, &net.UDPAddr{IP: net.IPv4zero, Port: 0})
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
				//utils.SetsockoptInt(fd, syscall.SOL_SOCKET, unix.SO_REUSEPORT, 1)
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
	Context        context.Context
	OnDisconnect   SocketCallbackFunc
	Conn           *quic.Conn
	framework.CloseableObject
	StreamChannelOperating
	channelEditLock sync.Mutex

	PacketPool *sync.Pool
}

func NewSocket(id string, channelCount int, onDisconnect SocketCallbackFunc) *Socket {
	sock := &Socket{
		Id:           id,
		ChannelCount: channelCount,
		Context:      context.Background(),
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
	if s.Conn != nil {
		_ = s.Conn.CloseWithError(0, "close")
	}
	return true
}

func (s *Socket) OnClosed() error {
	slog.Debug("socket 已经退出！", slog.String("id", s.Id))
	if s.OnDisconnect != nil {
		s.OnDisconnect(s)
	}
	return nil
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

func (s *Socket) CreatePacketPool(mtuSize uint16) *sync.Pool {
	return &sync.Pool{
		New: func() any {
			// 预分配大于 MTU 的固定内存，避免反复分配
			b := make([]byte, mtuSize)
			return &b
		},
	}
}

func (s *Socket) FecDecode(data []byte) *FECPacket {
	if len(data) < 12 {
		slog.Debug("收到无效的 Datagram：长度小于等于 4 字节", slog.Int("len", len(data)))
		return nil
	}
	result := &FECPacket{}
	result.ChannelId = int(data[0])
	if result.ChannelId < 0 || result.ChannelId >= s.ChannelCount {
		slog.Debug("收到无效的 Datagram：通道超出范围！", slog.Int("ChannelId", result.ChannelId))
		return nil
	}
	result.GroupId = binary.BigEndian.Uint64(data[1:9])
	result.ShardIdx = int(data[9])
	result.DataShards = int(data[10])
	result.ParityShards = int(data[11])
	result.Total = int(binary.BigEndian.Uint32(data[12:16]))
	result.Length = binary.BigEndian.Uint16(data[16:18])
	result.Payload = data[18:]
	if int(result.Length) != len(result.Payload) {
		slog.Debug("收到无效的 Datagram：数据包体大小不一致！", slog.Int("ChannelId", result.ChannelId),
			slog.Any("包体长度", result.Length), slog.Int("包体实际长度", len(result.Payload)))
		return nil
	}
	return result
}

func (s *Socket) HandleChannelStreamDatagram() {
	ctx, cancel := context.WithCancel(s.Context)
	defer cancel()
	slog.Info("开始接收 FEC Datagram 数据流...")
	isFirst := true
	for {
		if s.IsClosed {
			return
		}
		data, err := s.Conn.ReceiveDatagram(ctx)
		if err != nil {
			// 如果是连接关闭或 Context 取消，优雅退出循环，防止 CPU 100% 爆满
			if errors.Is(err, context.Canceled) || s.IsClosed {
				slog.Info("Datagram 接收协程正常退出")
				return
			}
			slog.Error("接收 Datagram 发生错误，退出循环", slog.Any("err", err))
			return
		}
		if isFirst {
			slog.Debug("收到首个Datagram数据包！")
			isFirst = false
		}
		packet := s.FecDecode(data)
		if packet != nil {
			sc := s.StreamChannels[packet.ChannelId]
			if sc == nil {
				return
			}
			if sc.Channel == nil {
				return
			}
			err = sc.FecDecode(packet)
			if err != nil {
				slog.Error("解码fec过程发生错误", slog.Any("err", err))
				return
			}
		}
	}
}

func (s *Socket) SetFecParam(channelId, dataShards, parityShards int) {
	if dataShards > 0 && parityShards > 0 {
		encoder, err := reedsolomon.New(dataShards, parityShards)
		if err == nil {
			s.StreamChannels[channelId].Encoder = encoder
			s.StreamChannels[channelId].DataShards = dataShards
			s.StreamChannels[channelId].ParityShards = parityShards
		}
	}
}

func (s *Socket) SendFecDatagram(channelId int, frameIndex int64, data []byte) error {
	total := len(data)
	shards, err := s.StreamChannels[channelId].Encoder.Split(data)
	if err != nil {
		return err
	}
	err = s.StreamChannels[channelId].Encoder.Encode(shards)
	if err != nil {
		slog.Debug("编码fec过程发生错误", slog.Int("channelId", channelId), slog.Any("err", err))
		return err
	}
	for index, shard := range shards {
		_ = s.sendFecShardDatagram(channelId, frameIndex, index, total, shard)
	}
	return nil
}

func (s *Socket) sendFecShardDatagram(channelId int, frameIndex int64, index, total int, data []byte) error {
	length := len(data)
	// 1. 从 Pool 获取一块已有的内存 (0 分配)
	bufPtr := s.PacketPool.Get().(*[]byte)
	defer s.PacketPool.Put(bufPtr) // 函数结束归还 Pool
	//  1. 检查 FEC Header 最小长度 (1 + 8 + 1 + 1 + 1 + 2 = 14 字节)
	packet := (*bufPtr)[:18+length]
	packet[0] = byte(channelId)
	binary.BigEndian.PutUint64(packet[1:], uint64(frameIndex))
	packet[9] = byte(index)
	packet[10] = byte(s.StreamChannels[channelId].DataShards)
	packet[11] = byte(s.StreamChannels[channelId].ParityShards)
	binary.BigEndian.PutUint32(packet[12:], uint32(total))
	binary.BigEndian.PutUint16(packet[16:], uint16(length))
	copy(packet[18:], data)
	return s.Conn.SendDatagram(packet)
}
