package network

import (
	"context"
	"encoding/binary"
	"errors"
	"github.com/DeleteElf/zero-net/framework"
	"github.com/DeleteElf/zero-net/framework/utils"
	"github.com/quic-go/quic-go"
	"log/slog"
	"math"
	"math/bits"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"
)

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
			return c.Control(func(fd uintptr) { utils.SetSocketReuse(fd) })
		},
	}
	config.SetMultipathTCP(false)
	conn, err := config.ListenPacket(context.Background(), STREAM_NETWORK_UDP, addr)
	if err != nil {
		return nil, err
	}
	slog.Debug("udp服务端口已开启", slog.Any("addr", conn.LocalAddr().String()))
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
	MtuPacketSize  uint16
	Context        context.Context
	OnDisconnect   SocketCallbackFunc
	Conn           *quic.Conn
	framework.CloseableObject
	StreamChannelOperating
	channelEditLock       sync.Mutex
	StreamConfigs         []StreamConfig
	PacketPool            *sync.Pool
	FecLimitPacketSize    int
	FecMinRequiredPackets int
	FecPacketIndex        map[uint32]*uint32
}

func NewSocket(id string, channelCount int, packetSize uint16, onDisconnect SocketCallbackFunc) *Socket {
	sock := &Socket{
		Id:             id,
		ChannelCount:   channelCount,
		MtuPacketSize:  packetSize,
		Context:        context.Background(),
		FecPacketIndex: make(map[uint32]*uint32),
	}
	sock.IsClosed = false
	sock.SetOnCloseHandler(sock)
	sock.OnDisconnect = onDisconnect
	return sock
}
func (s *Socket) CloseChannel(channelIndex int) bool {
	s.channelEditLock.Lock()
	defer s.channelEditLock.Unlock()
	if len(s.StreamChannels) > channelIndex && s.StreamChannels[channelIndex] != nil {
		_ = s.StreamChannels[channelIndex].Close()
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
			_ = s.StreamChannels[i].Close()
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
func (s *Socket) CreateChannels() {
	if s.StreamConfigs == nil {
		slog.Error("请先设置每个流的配置！")
		return
	}
	s.channelEditLock.Lock()
	defer s.channelEditLock.Unlock()
	s.StreamChannels = make([]*StreamChannel, s.ChannelCount) //创建通道列表切片
	for i := 0; i < s.ChannelCount; i++ {
		s.StreamChannels[i] = NewStreamChannel(s.Id, i) //make(chan StreamChannelData, 3) //创建通道实例
		s.StreamChannels[i].OnDisconnect = func(id string, index int) {
			if !s.IsClosed {
				s.channelEditLock.Lock()
				if index < len(s.StreamChannels) && s.StreamChannels[index] != nil {
					_ = s.StreamChannels[index].Close()
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
						_ = s.Close()
					}
				}
			}
		}
	}
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
	channel := s.StreamChannels[channelId]
	config := s.StreamConfigs[channelId]
	if config.EnableFec && len(data) > s.FecLimitPacketSize {
		return s.SendFecDatagram(channelId, data)
	} else {
		return channel.Send(data)
	}
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
		packet := s.GetFecDecodeInfo(data)
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
				continue
			}
		}
	}
}

func (s *Socket) InitFecParam(channelId int) error {
	config := &s.StreamConfigs[channelId]
	if config.EnableFec {
		if config.Type == Audio { //音频大约一个数据包是334大小左右，我们直接塞进一个里面
			atomic.AddUint64(&s.StreamChannels[channelId].FecGroups[0].NextGroupId, 1)
			total := config.DataShards + config.ParityShards
			s.StreamChannels[channelId].SharedShards = make([][]byte, total)
			s.StreamChannels[channelId].ParityShards = make([][]byte, config.ParityShards)
			for i := 0; i < config.ParityShards; i++ {
				s.StreamChannels[channelId].ParityShards[i] = make([]byte, config.FecPacketSize)
			}
			slog.Debug("音频奇偶校验缓存已经分配！")
		}
	}
	return nil // s.StreamChannels[channelId].BuildFecEncoder()
}

func (s *Socket) UpdateFecParam(channelId, dataShards, parityShards int) error {
	if s.StreamConfigs[channelId].EnableFec && dataShards > 0 && parityShards > 0 {
		s.StreamConfigs[channelId].DataShards = dataShards
		s.StreamConfigs[channelId].ParityShards = parityShards
		return nil
	}
	return nil
}

func (s *Socket) GetFecDecodeInfo(data []byte) *FecPacket {
	if len(data) < 12 {
		slog.Debug("收到无效的 Datagram：长度小于等于 4 字节", slog.Int("len", len(data)))
		return nil
	}
	result := &FecPacket{Ssrc: 0}
	if data[0]&0x80 == 0x80 { //标准的rtp包
		if data[1] == 97 || data[1] == 127 { //音频数据包
			//因为仅仅使用rtp packet来包装发送的数据，因此很多内容都是属于约定写死的内容
			result.ChannelId = 1
			result.DataShards = 4
			result.ParityShards = 2
			if s.StreamChannels[result.ChannelId].FecGroups[0].NextGroupId == 0 {
				return nil //直接丢弃即可
			}
			sequenceNumber := binary.BigEndian.Uint16(data[2:])
			result.Payload = data
			if data[1] == 127 {
				result.Length = uint16(len(data) - AudioHeaderLength)
				fecPkg, _ := ConvertByteToAudioFecPacket(data)
				result.ShardIdx = int(fecPkg.FecHeader.FecShardIndex) + result.DataShards
				result.GroupId = (uint64(sequenceNumber-uint16(fecPkg.FecHeader.FecShardIndex)-1) / uint64(result.DataShards)) + 1
			} else {
				result.Length = uint16(len(data) - RtpHeaderLength)
				result.ShardIdx = int(sequenceNumber) % result.DataShards
				result.GroupId = (uint64(sequenceNumber) / uint64(result.DataShards)) + 1
			}
			if result.GroupId < s.StreamChannels[result.ChannelId].FecGroups[0].NextGroupId { //已经解码成功的Id就不要了
				return nil
			}
			result.Total = int(result.Length) * result.DataShards
			//if int(result.Length) != len(result.Payload) {
			//	slog.Debug("收到无效的 Rtp audio Datagram：数据包体大小不一致！", slog.Int("ChannelId", result.ChannelId),
			//		slog.Any("包体长度", result.Length), slog.Int("包体实际长度", len(result.Payload)))
			//	return nil
			//}
		} else {
			result.Ssrc = binary.BigEndian.Uint32(data[8:]) //读取通道数据
			fecInfo := binary.LittleEndian.Uint32(data[28:])
			result.ChannelId = int(fecInfo & 0xF) // channelId: 占 4 位
			if result.ChannelId < 0 || result.ChannelId >= s.ChannelCount {
				slog.Debug("收到无效的 Datagram：通道超出范围！", slog.Int("ChannelId", result.ChannelId))
				return nil
			}
			result.GroupId = uint64(binary.BigEndian.Uint32(data[16:]))
			nextGroupId := s.StreamChannels[result.ChannelId].FecGroups[result.Ssrc].NextGroupId
			if result.GroupId < nextGroupId { //已经解码成功的Id就不要了
				return nil
			}
			result.Idr = (fecInfo>>11)&0x1 == 1              //idr 1 位
			if result.Idr && result.GroupId != nextGroupId { //如果是 关键帧，则移动当前数据到本帧，并丢弃前面的数据,执行追帧
				for i := nextGroupId; i < result.GroupId; i++ {
					delete(s.StreamChannels[result.ChannelId].FecGroups[result.Ssrc].Groups, i)
				}
				s.StreamChannels[result.ChannelId].FecGroups[result.Ssrc].NextGroupId = result.GroupId
			}
			//	binary.BigEndian.PutUint32(buffer[i][28:], uint32(dataShards<<22|i<<12|idrData<<11|fecPercentage<<4|channelId)) //FecInfo 增加idr信息、通道信息
			fecPercentage := int((fecInfo >> 4) & 0x7F)

			result.ShardIdx = int((fecInfo >> 12) & 0xFF)
			result.DataShards = int((fecInfo >> 22) & 0xFF)
			result.ParityShards = (result.DataShards*fecPercentage + 99) / 100
			result.Payload = data
			result.Length = uint16(len(result.Payload))
			result.Total = result.DataShards * int(result.Length)
			//if int(result.Length) != len(result.Payload) {
			//	slog.Debug("收到无效的 Rtp Datagram：数据包体大小不一致！", slog.Int("ChannelId", result.ChannelId),
			//		slog.Any("包体长度", result.Length), slog.Int("包体实际长度", len(result.Payload)))
			//	return nil
			//}
		}
	} else {
		chnId := int(data[0])
		if chnId < 0 || chnId >= s.ChannelCount {
			slog.Debug("收到无效的 Datagram：通道超出范围！", slog.Int("ChannelId", chnId))
			return nil
		}
		groupId := binary.BigEndian.Uint64(data[1:])
		nextGroupId := s.StreamChannels[chnId].FecGroups[0].NextGroupId
		if groupId < nextGroupId { //已经解码成功的Id就不要了
			return nil
		}
		//isIdr := int(data[9]) == 1
		////todo:未来还需要考虑，如果堆积太多的未处理，也需要清除，否则会内存溢出
		//if isIdr && groupId != nextGroupId { //如果是 关键帧，则移动当前数据到本帧，并丢弃前面的数据
		//	for i := nextGroupId; i < groupId; i++ {
		//		delete(s.StreamChannels[chnId].FecGroups[0].Groups, i)
		//	}
		//	s.StreamChannels[chnId].FecGroups[0].NextGroupId = groupId
		//}
		result.ChannelId = chnId
		result.GroupId = binary.BigEndian.Uint64(data[1:9])
		result.ShardIdx = int(data[9])
		result.DataShards = int(data[10])
		result.ParityShards = int(data[11])
		result.Total = int(binary.BigEndian.Uint32(data[12:16]))
		result.Length = binary.BigEndian.Uint16(data[16:FecPacketHeaderLength])
		result.Payload = data[FecPacketHeaderLength:]
		//slog.Debug("收到的数据包", slog.Int("channelId", chnId), slog.Int("total", result.Total),
		//	slog.Int("currentLength", int(result.Length)), slog.Int("dataLength", len(data)))
		if int(result.Length) != len(result.Payload) {
			slog.Debug("收到无效的 Datagram：数据包体大小不一致！", slog.Int("ChannelId", result.ChannelId),
				slog.Any("包体长度", result.Length), slog.Int("包体实际长度", len(result.Payload)),
				slog.Any("data", data))
			return nil
		}
	}
	return result
}

func (s *Socket) SendFecDatagram(channelId int, data []byte) (bool, error) {
	dataSize := len(data)  //通过包的长度，动态计算分片，这里总是取上整！
	if dataSize > 351900 { //计算255个分片  每个分片 1380的极限大小
		slog.Warn("发送的数据包大小超过 351900")
		return false, nil
	}
	channel := s.StreamChannels[channelId]
	config := &s.StreamConfigs[channelId]
	//主要执行逻辑是将数据放入分片中
	switch s.StreamConfigs[channelId].Type {
	case Audio: //音频模式特殊应用，他还不能有关键帧，如果超时丢弃了，数据包也是不要的，直接用下一个,使用的是标准的rtp数据包
		//另外，因为是一个一个发的，所以要保留其数据包头
		sequenceNumber := binary.BigEndian.Uint16(data[2:]) //取出序列号
		index := int(sequenceNumber % uint16(config.DataShards))
		if index == 0 {
			atomic.AddUint64(&channel.FecGroups[0].FrameIndex, 1)
		}

		channel.SharedShards[index] = data[RtpHeaderLength:dataSize]
		if data[0]&0x80 != 0x80 {
			slog.Debug("发送的数据包不是合格的rtp包,audio data")
		}
		_ = s.Conn.SendDatagram(data) //发送处理好的数据
		if index == config.DataShards-1 {
			encoder, err := channel.GetFecEncoder(config.DataShards, config.ParityShards)
			if err != nil {
				return false, err
			}
			totalShards := config.DataShards + config.ParityShards
			for i := config.DataShards; i < totalShards; i++ { //补发奇偶校验包
				if channel.SharedShards[i] == nil || cap(channel.SharedShards[i]) != dataSize {
					channel.SharedShards[i] = channel.ParityShards[i-config.DataShards][AudioHeaderLength : dataSize+12]
				}
				clear(channel.SharedShards[i]) //清除数据防止脏读
			}
			err = encoder.Encode(channel.SharedShards)
			if err != nil {
				slog.Debug("编码fec过程发生错误", slog.Int("channelId", channelId),
					slog.Any("sequenceNumber", sequenceNumber), slog.Int("length", len(channel.SharedShards)),
					slog.Int("DataShards", config.DataShards), slog.Int("ParityShards", config.ParityShards),
					slog.Any("err", err))
				return false, err
			}
			lastRtp, _ := ConvertByteToRtpPacket(data)
			for i := 0; i < config.ParityShards; i++ { //补发奇偶校验包
				audioFecPacket, _ := ConvertByteToAudioFecPacket(channel.ParityShards[i])
				audioFecPacket.Rtp.Header = lastRtp.Header
				//在很多实时音视频系统（如 WebRTC、RTP/FEC 扩展协议）中，为了避免 FEC 冗余包与正常的音频（如 97 或 111）或视频 Payload Type 冲突，通常会选择 动态区间的边界值（如 127） 专门作为 FEC 冗余包的 Dynamic Payload Type。
				audioFecPacket.Rtp.PacketType = 127
				audioFecPacket.Rtp.Ssrc = lastRtp.Ssrc
				audioFecPacket.Rtp.Timestamp = lastRtp.Timestamp
				audioFecPacket.Rtp.SequenceNumber = bits.ReverseBytes16(bits.ReverseBytes16(lastRtp.SequenceNumber) + uint16(i) + 1)

				audioFecPacket.FecHeader.FecShardIndex = uint8(i)
				audioFecPacket.FecHeader.BaseTimestamp = lastRtp.Timestamp
				audioFecPacket.FecHeader.BaseSequenceNumber = lastRtp.SequenceNumber
				audioFecPacket.FecHeader.Ssrc = lastRtp.Ssrc
				audioFecPacket.FecHeader.PayloadType = 97
				if channel.ParityShards[i][0]&0x80 != 0x80 {
					slog.Debug("发送的数据包不是合格的rtp包,audio parity")
				}
				_ = s.Conn.SendDatagram(channel.ParityShards[i][:dataSize]) //发送处理好的数据
			}
		}
	case Video: //支持媒体流传输的特殊协议
		length := len(data)
		blockSize := int(s.StreamConfigs[channelId].FecPacketSize)
		firstPacketHeader := (*VideoPacketHeader)(unsafe.Pointer(&data[0]))
		fecPercentage := int(firstPacketHeader.Packet.FecInfo >> 4 & 0xff)
		idrData := int(firstPacketHeader.Packet.FecInfo >> 11 & 0x1)
		lowSeq := firstPacketHeader.Packet.StreamPacketIndex >> 8
		ssrc := binary.BigEndian.Uint32(data[8:]) //bits.ReverseBytes32(firstPacketHeader.Rtp.Ssrc)
		if s.FecPacketIndex[ssrc] == nil {
			s.FecPacketIndex[ssrc] = new(uint32)
		}
		packetIndex := atomic.AddUint32(s.FecPacketIndex[ssrc], 1)
		pad := length%blockSize != 0 //是否需要补零
		dataShards := (length + (blockSize - 1)) / blockSize
		parityShards := (dataShards*fecPercentage + 99) / 100

		if parityShards < s.FecMinRequiredPackets && fecPercentage != 0 {
			parityShards = s.FecMinRequiredPackets
			fecPercentage = (100 * parityShards) / dataShards
			slog.Debug("编码fec过程因fec parity shard 太少，增加fec百分比", slog.Int("百分比", fecPercentage))
		}
		totalShards := dataShards + parityShards
		shards := make([][]byte, totalShards)
		buffer := make([][]byte, totalShards)
		if pad {
			bufPtr := s.PacketPool.Get().(*[]byte)
			defer s.PacketPool.Put(bufPtr) // 函数结束归还 Pool
			buffer[dataShards-1] = (*bufPtr)[:blockSize]
			clear(buffer[dataShards-1])
		}
		for i := 0; i < dataShards; i++ {
			offset := i * blockSize
			if i == dataShards-1 && pad { //如果是最后一个，并且需要pad时，我们进行补零操作
				copy(buffer[i], data[offset:]) // 拷贝剩余数据，末尾自动补 0
			} else {
				buffer[i] = data[offset : offset+blockSize] //取数据切片,实现零拷贝
			}
			shards[i] = buffer[i][VideoHeaderLength:blockSize] //取数据切片

			binary.BigEndian.PutUint32(buffer[i][16:], packetIndex) //StreamPacketIndex
			//这个结构 支持fecPercentage最大127，channelId最大15
			binary.LittleEndian.PutUint32(buffer[i][28:], uint32(dataShards<<22|i<<12|idrData<<11|fecPercentage<<4|channelId)) //FecInfo 增加idr信息、通道信息
			binary.BigEndian.PutUint16(buffer[i][2:], uint16(lowSeq+uint32(i)))                                                //SequenceNumber
		}
		if fecPercentage != 0 {
			usedBuffers := make([]*[]byte, 0, parityShards)
			defer func() {
				for _, bufPtr := range usedBuffers {
					if bufPtr != nil {
						s.PacketPool.Put(bufPtr)
					}
				}
			}()
			for i := dataShards; i < totalShards; i++ {
				bufPtr := s.PacketPool.Get().(*[]byte)
				usedBuffers = append(usedBuffers, bufPtr)
				buffer[i] = (*bufPtr)[:blockSize]
				clear(buffer[i])
				packetHeader := (*VideoPacketHeader)(unsafe.Pointer(&buffer[i][0]))

				packetHeader.Packet.FrameIndex = firstPacketHeader.Packet.FrameIndex
				binary.BigEndian.PutUint32(buffer[i][16:], packetIndex)                                                            //StreamPacketIndex
				binary.LittleEndian.PutUint32(buffer[i][28:], uint32(dataShards<<22|i<<12|idrData<<11|fecPercentage<<4|channelId)) //FecInfo 增加idr信息、通道信息
				binary.BigEndian.PutUint16(buffer[i][2:], uint16(lowSeq+uint32(i)))                                                //SequenceNumber
				packetHeader.Packet.MultiFecBlocks = firstPacketHeader.Packet.MultiFecBlocks
				packetHeader.Packet.MultiFecFlags = 0

				packetHeader.Rtp.Header = firstPacketHeader.Rtp.Header
				packetHeader.Rtp.Timestamp = firstPacketHeader.Rtp.Timestamp
				packetHeader.Rtp.Ssrc = firstPacketHeader.Rtp.Ssrc

				// Parity 包的 Payload 区域直接映射给 encoder，准备接收计算结果
				shards[i] = buffer[i][VideoHeaderLength:blockSize]
			}
			encoder, err := channel.GetFecEncoder(dataShards, parityShards)
			if err != nil {
				return false, err
			}
			err = encoder.Encode(shards)
			if err != nil {
				slog.Debug("编码fec过程发生错误", slog.Int("channelId", channelId),
					slog.Int("DataShards", dataShards), slog.Int("ParityShards", parityShards),
					slog.Any("err", err))
				return false, err
			}
		} else {
			//slog.Debug("fec 分片百分比为0")
		}
		for index, shard := range buffer {
			if shard[0]&0x80 != 0x80 {
				slog.Debug("发送的数据包不是合格的rtp包", slog.Int("数据长度", length),
					slog.Int("DataShards", dataShards), slog.Int("ParityShards", parityShards),
					slog.Int("index", index))
			}
			_ = s.Conn.SendDatagram(shard) //发送处理好的数据
		}
	default: //数据分割模式
		targetDataShards := int(math.Ceil(float64(dataSize) / float64(s.StreamConfigs[channelId].FecPacketSize-FecPacketHeaderLength)))
		targetParityShards := int(math.Ceil(float64(targetDataShards) / float64(s.StreamConfigs[channelId].DataShards) * float64(s.StreamConfigs[channelId].ParityShards)))
		totalShards := targetDataShards + targetParityShards
		if totalShards > 255 { //为了防止数据分片过大，我们进行大小的压缩
			targetParityShards = 255 - targetDataShards
		}
		encoder, err := channel.GetFecEncoder(targetDataShards, targetParityShards)
		if err != nil {
			return false, err
		}
		shards, err := encoder.Split(data)
		if err != nil {
			return false, err
		}
		err = encoder.Encode(shards)
		if err != nil {
			slog.Debug("编码fec过程发生错误", slog.Int("channelId", channelId),
				slog.Int("DataShards", targetDataShards), slog.Int("ParityShards", targetParityShards),
				slog.Any("err", err))
			return false, err
		}
		for index, shard := range shards {
			_ = s.sendFecShardDatagram(channelId, channel.FecGroups[0].FrameIndex, index, dataSize, targetDataShards, targetParityShards, shard)
		}
	}
	return true, nil
}

func (s *Socket) sendFecShardDatagram(channelId int, frameIndex uint64, index, total, dataShards, parityShards int, data []byte) error {
	length := len(data)
	// 1. 从 Pool 获取一块已有的内存 (0 分配)
	bufPtr := s.PacketPool.Get().(*[]byte)
	defer s.PacketPool.Put(bufPtr) // 函数结束归还 Pool
	//  1. 检查 FEC Header 最小长度 (1 + 8 + 1 + 1 + 1 + 2 = 14 字节)
	packet := (*bufPtr)[:FecPacketHeaderLength+length]
	packet[0] = byte(channelId)
	binary.BigEndian.PutUint64(packet[1:], frameIndex)
	packet[9] = byte(index)
	packet[10] = byte(dataShards)
	packet[11] = byte(parityShards)
	binary.BigEndian.PutUint32(packet[12:], uint32(total))
	binary.BigEndian.PutUint16(packet[16:], uint16(length))
	copy(packet[FecPacketHeaderLength:], data)

	//slog.Debug("发送的数据包", slog.Int("channelId", channelId), slog.Int("total", total),
	//	slog.Int("currentLength", length), slog.Int("dataLength", len(packet)))
	return s.Conn.SendDatagram(packet)
}
