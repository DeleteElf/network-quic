package network

import (
	"context"
	"encoding/binary"
	"errors"
	"github.com/DeleteElf/zero-net/framework"
	"github.com/DeleteElf/zero-net/framework/utils"
	"github.com/quic-go/quic-go"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
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
	FecMinRequiredPackets uint8
	FecPacketIndex        map[uint32]*uint32
	LastSendTime          time.Time
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
	sock.FecLimitPacketSize = 100
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
			sc := s.StreamChannels[packet.Header.ChannelId]
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
			total := config.DataShards + config.ParityShards
			fecGroup := s.StreamChannels[channelId].FecGroups[0]
			fecGroup.SharedShards = make([][]byte, total)
			fecGroup.ParityShards = make([][]byte, config.ParityShards)
			for i := uint8(0); i < config.ParityShards; i++ {
				fecGroup.ParityShards[i] = make([]byte, config.FecPacketSize)
			}
			slog.Debug("音频奇偶校验缓存已经分配！")
		}
	}
	return nil // s.StreamChannels[channelId].BuildFecEncoder()
}

func (s *Socket) UpdateFecParam(channelId int, dataShards, parityShards uint8) error {
	if s.StreamConfigs[channelId].EnableFec && dataShards > 0 && parityShards > 0 {
		s.StreamConfigs[channelId].DataShards = dataShards
		s.StreamConfigs[channelId].ParityShards = parityShards
		return nil
	}
	return nil
}

func (s *Socket) GetFecDecodeInfo(data []byte) *FecPacket {
	size := len(data)
	if size < RtpHeaderLength {
		slog.Debug("收到无效的 Datagram：长度小于等于 4 字节", slog.Int("len", size))
		return nil
	}
	result := &FecPacket{}
	switch data[0] {
	case RtpHeader:
		if data[1] == 97 || data[1] == 127 { //音频数据包
			//因为仅仅使用rtp packet来包装发送的数据，因此很多内容都是属于约定写死的内容
			result.Header.ChannelId = 1
			result.Header.DataShards = 4
			result.Header.ParityShards = 2
			sequenceNumber := binary.BigEndian.Uint16(data[2:])
			result.Payload = data
			if data[1] == 127 {
				result.Header.Length = uint16(size - AudioHeaderLength)
				fecShardIndex := data[12]
				result.Header.ShardIdx = fecShardIndex + result.Header.DataShards
				result.Header.GroupId = (uint64(sequenceNumber-uint16(fecShardIndex)-1) / uint64(result.Header.DataShards)) + 1
			} else {
				result.Header.Length = uint16(size - RtpHeaderLength)
				result.Header.ShardIdx = uint8(sequenceNumber % uint16(result.Header.DataShards))
				result.Header.GroupId = (uint64(sequenceNumber) / uint64(result.Header.DataShards)) + 1
			}
			fecGroup := s.StreamChannels[result.Header.ChannelId].FecGroups[0]
			if fecGroup.NextGroupId == 0 {
				atomic.StoreUint64(&fecGroup.NextGroupId, result.Header.GroupId) //如果一直没开始，则开始立即使用最新的，丢弃之前的
			}
			if result.Header.GroupId < fecGroup.NextGroupId { //已经解码成功的Id就不要了
				return nil
			}
			result.Header.Total = uint32(result.Header.Length) * uint32(result.Header.DataShards)
		} else {
			slog.Debug("未支持的rtp数据包")
		}
	case VideoHeader:
		if size < VideoHeaderLength {
			slog.Debug("收到无效的video rtp数据包", slog.Int("len", size))
			return nil
		}
		fecInfo := binary.LittleEndian.Uint32(data[28:])
		result.Header.ChannelId = uint8(fecInfo & 0xF) // channelId: 占 4 位
		if result.Header.ChannelId < 0 || result.Header.ChannelId >= uint8(s.ChannelCount) {
			slog.Debug("收到无效的 Datagram：通道超出范围！", slog.Any("ChannelId", result.Header.ChannelId))
			return nil
		}
		result.Header.Ssrc = uint8(binary.BigEndian.Uint32(data[8:])) //读取通道数据
		if result.Header.Ssrc > 1 {                                   //暂时不支持更大的
			slog.Debug("无效的ssrc", slog.Any("ssrc", result.Header.Ssrc))
			return nil
		}
		fecGroupMap, exists := s.StreamChannels[result.Header.ChannelId].FecGroups[result.Header.Ssrc]
		if !exists {
			fecGroupMap = NewFecGroupsMap()
			s.StreamChannels[result.Header.ChannelId].FecGroups[result.Header.Ssrc] = fecGroupMap
			atomic.AddUint64(&fecGroupMap.NextGroupId, 1)
		}
		result.Header.GroupId = uint64(binary.BigEndian.Uint32(data[16:]))

		slog.Debug("step 20", slog.Any("ChannelId", result.Header.ChannelId), slog.Any("Ssrc", result.Header.Ssrc))
		nextGroupId := fecGroupMap.NextGroupId
		if result.Header.GroupId < nextGroupId { //已经解码成功的Id就不要了
			return nil
		}
		result.Header.Idr = uint8(fecInfo >> 11 & 0x1)                      //idr 1 位
		if result.Header.Idr == 1 && result.Header.GroupId != nextGroupId { //如果是 关键帧，则移动当前数据到本帧，并丢弃前面的数据,执行追帧
			for i := nextGroupId; i < result.Header.GroupId; i++ {
				delete(fecGroupMap.Groups, i)
			}
			fecGroupMap.NextGroupId = result.Header.GroupId
		}
		//	binary.BigEndian.PutUint32(buffer[i][28:], uint32(dataShards<<22|i<<12|idrData<<11|fecPercentage<<4|channelId)) //FecInfo 增加idr信息、通道信息
		result.Header.Header = data[0]
		result.Header.Type = data[1]
		fecPercentage := uint8((fecInfo >> 4) & 0x7F)
		result.Header.ShardIdx = uint8((fecInfo >> 12) & 0xFF)
		result.Header.DataShards = uint8((fecInfo >> 22) & 0xFF)
		result.Header.ParityShards = uint8(utils.Ceil(uint64(result.Header.DataShards)*uint64(fecPercentage), 100))
		result.Payload = data
		result.Header.Length = uint16(size - VideoHeaderLength)
		result.Header.Total = uint32(result.Header.DataShards) * uint32(result.Header.Length)
	default:
		if size < FecPacketHeaderLength {
			slog.Debug("收到无效的fec数据包", slog.Int("len", size))
			return nil
		}
		result.Header.Header = data[0]
		result.Header.Type = data[1]
		result.Header.ChannelId = data[2]
		if result.Header.Header != CustomFecHeader || result.Header.Type != CustomMessageType {
			slog.Debug("收到无效的Datagram！", slog.Any("ChannelId", result.Header.ChannelId),
				slog.Any("头信息", result.Header.Header), slog.Any("消息类型", result.Header.Type))
			return nil
		}
		result.Header.Ssrc = data[3]
		result.Header.FrameId = binary.BigEndian.Uint16(data[4:])
		result.Header.Timestamp = binary.BigEndian.Uint64(data[6:])
		result.Header.GroupId = binary.BigEndian.Uint64(data[14:])
		result.Header.Idr = data[22]
		result.Header.ShardIdx = data[23]
		result.Header.DataShards = data[24]
		result.Header.ParityShards = data[25]

		result.Header.Total = binary.BigEndian.Uint32(data[26:])
		result.Header.Length = binary.BigEndian.Uint16(data[30:])
		result.Payload = data
	}
	return result
}

func (s *Socket) SafeWaitMillisecond(timeout time.Duration) {
	s.channelEditLock.Lock()
	if s.LastSendTime.Add(timeout).After(time.Now()) {
		time.Sleep(time.Millisecond)
	}
	s.LastSendTime = time.Now()
	s.channelEditLock.Unlock()
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
		index := uint8(sequenceNumber % uint16(config.DataShards))
		fecGroup := channel.FecGroups[0]
		fecGroup.SharedShards[index] = data[RtpHeaderLength:dataSize]
		if data[0] != RtpHeader {
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
				if fecGroup.SharedShards[i] == nil || cap(fecGroup.SharedShards[i]) != dataSize {
					fecGroup.SharedShards[i] = fecGroup.ParityShards[i-config.DataShards][AudioHeaderLength : dataSize+12]
				}
				clear(fecGroup.SharedShards[i]) //清除数据防止脏读
			}
			err = encoder.Encode(fecGroup.SharedShards)
			if err != nil {
				slog.Debug("编码fec过程发生错误", slog.Int("channelId", channelId),
					slog.Any("sequenceNumber", sequenceNumber), slog.Int("length", len(fecGroup.SharedShards)),
					slog.Any("DataShards", config.DataShards), slog.Any("ParityShards", config.ParityShards),
					slog.Any("err", err))
				return false, err
			}
			for i := uint8(0); i < config.ParityShards; i++ { //补发奇偶校验包
				fecGroup.ParityShards[i][0] = data[0]
				fecGroup.ParityShards[i][1] = 127
				newNumber := binary.BigEndian.Uint16(data[2:]) + uint16(i) + 1
				binary.BigEndian.PutUint16(fecGroup.ParityShards[i][2:], newNumber)
				copy(fecGroup.ParityShards[i][4:12], data[4:12])
				fecGroup.ParityShards[i][12] = i
				fecGroup.ParityShards[i][13] = 97
				copy(fecGroup.ParityShards[i][14:24], data[2:12])
				_ = s.Conn.SendDatagram(fecGroup.ParityShards[i][:dataSize]) //发送处理好的数据
			}
		}
	case Video: //支持媒体流传输的特殊协议
		length := len(data)
		blockSize := int(s.StreamConfigs[channelId].FecPacketSize)
		fecInfo := binary.LittleEndian.Uint32(data[28:32])
		fecPercentage := uint8(fecInfo >> 4 & 0x7f)
		idrData := int(fecInfo >> 11 & 0x1)
		lowSeq := binary.LittleEndian.Uint32(data[16:20]) >> 8
		ssrc := binary.BigEndian.Uint32(data[8:]) //bits.ReverseBytes32(firstPacketHeader.Rtp.Ssrc)
		if s.FecPacketIndex[ssrc] == nil {
			s.FecPacketIndex[ssrc] = new(uint32)
		}
		packetIndex := atomic.AddUint32(s.FecPacketIndex[ssrc], 1)
		pad := length%blockSize != 0 //是否需要补零
		dataShards := uint8(utils.Ceil(length, blockSize))
		parityShards := uint8(utils.Ceil(int(dataShards)*int(fecPercentage), 100))

		if parityShards < s.FecMinRequiredPackets && fecPercentage != 0 {
			parityShards = s.FecMinRequiredPackets
			fecPercentage = (100 * parityShards) / dataShards //取下整
			slog.Debug("编码fec过程因fec parity shard 太少，增加fec百分比", slog.Any("百分比", fecPercentage))
		}
		//slog.Debug("fec分片结果", slog.Any("dataShards", dataShards), slog.Any("parityShards", parityShards))
		totalShards := dataShards + parityShards
		shards := make([][]byte, totalShards)
		buffer := make([][]byte, totalShards)
		if pad {
			bufPtr := s.PacketPool.Get().(*[]byte)
			defer s.PacketPool.Put(bufPtr) // 函数结束归还 Pool
			buffer[dataShards-1] = (*bufPtr)[:blockSize]
			clear(buffer[dataShards-1])
		}
		for i := 0; i < int(dataShards); i++ {
			offset := i * blockSize
			if i == int(dataShards)-1 && pad { //如果是最后一个，并且需要pad时，我们进行补零操作
				copy(buffer[i], data[offset:]) // 拷贝剩余数据，末尾自动补 0
			} else {
				buffer[i] = data[offset : offset+blockSize] //取数据切片,实现零拷贝
			}
			shards[i] = buffer[i][VideoHeaderLength:blockSize] //取数据切片

			binary.BigEndian.PutUint32(buffer[i][16:], packetIndex) //StreamPacketIndex
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
				copy(buffer[i][:VideoHeaderLength], buffer[0][:VideoHeaderLength]) //拷贝头部数据
				binary.LittleEndian.PutUint32(buffer[i][28:],
					uint32(dataShards)<<22|uint32(i)<<12|uint32(idrData)<<11|uint32(fecPercentage)<<4|uint32(channelId)) //FecInfo 增加idr信息、通道信息
				binary.BigEndian.PutUint16(buffer[i][2:], uint16(lowSeq+uint32(i))) //SequenceNumber
				buffer[i][24] = 0                                                   //这个属性是什么并不重要
				buffer[i][26] = 0
				shards[i] = buffer[i][VideoHeaderLength:blockSize]
			}
			encoder, err := channel.GetFecEncoder(dataShards, parityShards)
			if err != nil {
				return false, err
			}
			err = encoder.Encode(shards)

			if err != nil {
				slog.Debug("编码fec过程发生错误", slog.Int("channelId", channelId),
					slog.Any("DataShards", dataShards), slog.Any("ParityShards", parityShards),
					slog.Any("err", err))
				return false, err
			}
		} else {
			slog.Debug("fec 分片百分比为0")
		}
		s.SafeWaitMillisecond(time.Millisecond)
		for index, shard := range buffer {
			if shard[0] != VideoHeader {
				slog.Debug("发送的数据包不是合格的rtp包", slog.Int("数据长度", length),
					slog.Any("DataShards", dataShards), slog.Any("ParityShards", parityShards),
					slog.Int("index", index))
			}
			_ = s.Conn.SendDatagram(shard) //发送处理好的数据
		}
	default: //数据分割模式
		frameIndex := atomic.AddUint64(&channel.FecGroups[0].FrameIndex, 1)
		cfg := s.StreamConfigs[channelId]
		maxPayloadSize := int(cfg.FecPacketSize - FecPacketHeaderLength)
		targetDataShards := uint8(utils.Ceil(dataSize, maxPayloadSize))
		targetParityShards := utils.Ceil(targetDataShards, cfg.DataShards) * cfg.ParityShards
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
				slog.Any("DataShards", targetDataShards), slog.Any("ParityShards", targetParityShards),
				slog.Any("err", err))
			return false, err
		}
		s.SafeWaitMillisecond(time.Millisecond)
		for index, shard := range shards {
			_ = s.sendFecShardDatagram(uint8(channelId), 0, 0, uint8(index),
				targetDataShards, targetParityShards, uint16(frameIndex), frameIndex, uint32(dataSize), shard)
		}
	}
	return true, nil
}

func (s *Socket) sendFecShardDatagram(channelId, ssrc, idr, index, dataShards, parityShards uint8,
	frameIndex uint16, groupId uint64, total uint32, data []byte) error {
	length := len(data)
	size := FecPacketHeaderLength + length
	// 1. 从 Pool 获取一块已有的内存 (0 分配)
	bufPtr := s.PacketPool.Get().(*[]byte)
	defer s.PacketPool.Put(bufPtr) // 函数结束归还 Pool
	buf := (*bufPtr)[:size]
	clear(buf) //清除数据
	buf[0] = CustomFecHeader
	buf[1] = CustomMessageType
	buf[2] = channelId
	buf[3] = ssrc
	binary.BigEndian.PutUint16(buf[4:], frameIndex)
	binary.BigEndian.PutUint64(buf[6:], uint64(time.Now().UnixMilli()))
	binary.BigEndian.PutUint64(buf[14:], groupId)
	buf[22] = idr
	buf[23] = index
	buf[24] = dataShards
	buf[25] = parityShards
	binary.BigEndian.PutUint32(buf[26:], total)
	binary.BigEndian.PutUint16(buf[30:], uint16(length))
	copy(buf[FecPacketHeaderLength:], data)

	return s.Conn.SendDatagram(buf[:size])
}
