package network

import (
	"encoding/binary"
	"errors"
	"unsafe"
)

const (
	FecPacketHeaderLength    = 32
	FecLimitPacketSize       = 100
	NetMtuPacketSize         = 1400
	VideoHeaderLength        = 32
	AudioHeaderLength        = 24
	RtpHeaderLength          = 12
	NvidiaPacketHeaderLength = 16
	// RtpHeader 普通rtp标准头
	RtpHeader = 0x80
	// RtpHeaderFlagExtension rtp头携带了扩展的信息
	RtpHeaderFlagExtension = 0x10
	// VideoHeader rtp头携带扩展信息后的头信息
	VideoHeader = RtpHeader | RtpHeaderFlagExtension

	CustomFecHeader   = 0x81
	CustomMessageType = 0x11
)

// FecPacketHeader 自定义分Fec数据包
type FecPacketHeader struct {
	//数据头类型
	Header uint8
	//数据类型
	Type uint8
	//通道的编号，主要用于路由分发，1个字节
	ChannelId uint8
	//同步源id
	Ssrc uint8
	//如果涉及多分块，则存在一帧内，多个group id,起始位置4
	FrameId uint16
	//数据包时间,起始位置6
	Timestamp uint64
	//分组的编号，主要用于重组，8个字节，考虑到可能会播放很久,GroupId并不等于FrameIndex，一个数据包可能被拆成多个分组,起始位置14
	GroupId uint64
	//是否关键帧，如果不是关键帧，则只能依赖超时来跳过
	Idr uint8
	//shard的索引 1个字节
	ShardIdx uint8
	//核心数据分片数
	DataShards uint8
	//fec矩阵分片数
	ParityShards uint8
	//数据总长度,起始位置26
	Total uint32
	//数据包体长度 2个字节,起始位置30
	Length uint16
}

type FecPacket struct {
	Header FecPacketHeader
	//数据内容载体
	Payload []byte
}

type RtpPacket struct {
	Header         uint8
	PacketType     uint8
	SequenceNumber uint16
	Timestamp      uint32
	Ssrc           uint32
}

type NvidiaVideoPacket struct {
	StreamPacketIndex uint32
	FrameIndex        uint32
	Flags             uint8
	ExtraFlags        uint8
	MultiFecFlags     uint8
	MultiFecBlocks    uint8
	FecInfo           uint32
}

type VideoPacketHeader struct {
	Rtp      RtpPacket
	Reserved [4]byte
	Packet   NvidiaVideoPacket
}

type VideoPacket struct {
	Header  VideoPacketHeader
	Payload []byte // 直接用 slice 存放后续的二进制数据
}

type AudioFecHeader struct {
	FecShardIndex      uint8
	PayloadType        uint8
	BaseSequenceNumber uint16
	BaseTimestamp      uint32
	Ssrc               uint32
}

type AudioFecPacket struct {
	Rtp       RtpPacket
	FecHeader AudioFecHeader
	Payload   []byte
}

var RtpPacketTooShort = errors.New("Rtp数据包长度不足，转换失败")

// ConvertByteToRtpPacket 将数据直接转成RtpPacket结构，要求数据结构对齐，修改结构变量即修改数据变量，windows这里会默认执行Little-Endian
func ConvertByteToRtpPacket(data []byte) (*RtpPacket, error) {
	if len(data) < RtpHeaderLength {
		return nil, RtpPacketTooShort
	}
	var p RtpPacket
	p.Header = data[0]
	p.PacketType = data[1]
	p.SequenceNumber = binary.BigEndian.Uint16(data[2:4])
	p.Timestamp = binary.BigEndian.Uint32(data[4:8])
	p.Ssrc = binary.BigEndian.Uint32(data[8:12])
	return &p, nil
}

func ConvertByteToAudioFecPacket(data []byte) (*AudioFecPacket, error) {
	if len(data) < AudioHeaderLength {
		return nil, RtpPacketTooShort
	}
	return &AudioFecPacket{
		Rtp: RtpPacket{
			Header:         data[0],
			PacketType:     data[1],
			SequenceNumber: binary.BigEndian.Uint16(data[2:4]),
			Timestamp:      binary.BigEndian.Uint32(data[4:8]),
			Ssrc:           binary.BigEndian.Uint32(data[8:12]),
		},
		FecHeader: AudioFecHeader{
			FecShardIndex:      data[12],
			PayloadType:        data[13],
			BaseSequenceNumber: binary.BigEndian.Uint16(data[14:16]),
			BaseTimestamp:      binary.BigEndian.Uint32(data[16:20]),
			Ssrc:               binary.BigEndian.Uint32(data[20:24]),
		},
		Payload: data[24:],
	}, nil
}

func ConvertByteToFecPacketHeader(data []byte) (*FecPacketHeader, error) {
	if len(data) >= FecPacketHeaderLength {
		var h FecPacketHeader
		h.Header = data[0]
		h.Type = data[1]
		h.ChannelId = data[2]
		h.Ssrc = data[3]
		h.FrameId = binary.BigEndian.Uint16(data[4:6])
		h.Timestamp = binary.BigEndian.Uint64(data[6:14])
		h.GroupId = binary.BigEndian.Uint64(data[14:22])
		h.Idr = data[22]
		h.ShardIdx = data[23]
		h.DataShards = data[24]
		h.ParityShards = data[25]
		h.Total = binary.BigEndian.Uint32(data[26:30])
		h.Length = binary.BigEndian.Uint16(data[30:32])
		return &h, nil
	}
	return nil, errors.New("转换失败！")
}

// ConvertByteToVideoPacket 将数据直接转成VideoPacket结构，要求数据结构对齐，修改结构变量即修改数据变量，windows这里会默认执行Little-Endian
func ConvertByteToVideoPacket(data []byte) (*VideoPacket, error) {
	if len(data) >= VideoHeaderLength {
		return &VideoPacket{
			Header: VideoPacketHeader{
				Rtp: RtpPacket{
					Header:         data[0],
					PacketType:     data[1],
					SequenceNumber: binary.BigEndian.Uint16(data[2:4]),
					Timestamp:      binary.BigEndian.Uint32(data[4:8]),
					Ssrc:           binary.BigEndian.Uint32(data[8:12]),
				},
				Reserved: [4]byte{data[12], data[13], data[14], data[15]},
				Packet: NvidiaVideoPacket{
					StreamPacketIndex: binary.LittleEndian.Uint32(data[16:20]),
					FrameIndex:        binary.BigEndian.Uint32(data[20:24]),
					Flags:             data[24],
					ExtraFlags:        data[25],
					MultiFecFlags:     data[26],
					MultiFecBlocks:    data[27],
					FecInfo:           binary.LittleEndian.Uint32(data[28:32]),
				},
			},
			Payload: data[VideoHeaderLength:],
		}, nil
	}
	return nil, errors.New("转换失败！")
}

func ConvertToByte(p unsafe.Pointer, size uintptr) []byte {
	// 转换指针类型并生成 slice，使用零拷贝
	//return (*[1 << 30]byte)(p)[:size:size] //旧的写法
	return unsafe.Slice((*byte)(p), size) //1.17之后的新写法
}

func ConvertObjectToByte[T any](t *T) []byte {
	if t == nil {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(t)), unsafe.Sizeof(*t))
	//return ConvertToByte(unsafe.Pointer(t))
}

// RebuildRtpPacket 通过fec解码后的数据重建rtp数据包
//
//   - param buffer 通过缓存池建立的数据缓存
//   - param header 同个fec分组的其他数据头，用于样本恢复
//   - param data fec解码后生成的顺序数据包，仅包含数据部分
//   - param shardIndex	当前需要重建的数据包所在的分片索引
//   - param dataShards	当前分组的数据分片总数
//
// return 返回构建好的新数据包
func RebuildRtpPacket(header, data []byte, shardIndex, dataShards uint8) []byte {
	if header[1] == 0x61 || header[1] == 0x7f {
		total := RtpHeaderLength + len(data)
		buffer := make([]byte, total)               //循环中，且释放时机不好确定，不使用sync.Pool
		copy(buffer[0:], header[0:RtpHeaderLength]) //仅拷贝加入rtp包即可
		copy(buffer[RtpHeaderLength:], data)
		switch header[1] {
		case 0x61: //标准音频
			sequenceNumber := binary.BigEndian.Uint16(buffer[2:])
			sequenceNumber = sequenceNumber - sequenceNumber%uint16(dataShards) + uint16(shardIndex) //计算当前的包
			binary.BigEndian.PutUint16(buffer[2:], sequenceNumber)
		case 0x7f: //动态音频
			sequenceNumber := binary.BigEndian.Uint16(buffer[14:]) - uint16(dataShards) + 1 + uint16(shardIndex)
			buffer[1] = 0x61 //通过动态音频获取的 packetType为127，我们需要修改成97
			binary.BigEndian.PutUint16(buffer[2:], sequenceNumber)
		default: //其他都是视频 视频数据的rtp包数据都是一样的
			//sequenceNumber 和 fec info 需要重建
		}
		return buffer
	} else {
		total := VideoHeaderLength + len(data)
		buffer := make([]byte, total)                 //循环中，且释放时机不好确定，不使用sync.Pool
		copy(buffer[0:], header[0:VideoHeaderLength]) //仅拷贝加入rtp包即可
		copy(buffer[VideoHeaderLength:], data)
		oldFecInfo := binary.LittleEndian.Uint32(buffer[28:])
		oldShardIndex := uint16((oldFecInfo >> 12) & 0x3FF)
		newFecInfo := (oldFecInfo & 0xFFF) | (uint32(dataShards) << 22) | (uint32(shardIndex) << 12) // 清空高 20 位（保留低 12 位 0x00000FFF），并填入新的 dataShards 和 shardIndex
		binary.LittleEndian.PutUint32(buffer[28:], newFecInfo)
		sequenceNumber := binary.BigEndian.Uint16(buffer[2:]) - oldShardIndex + uint16(shardIndex)
		binary.BigEndian.PutUint16(buffer[2:], sequenceNumber)
		//补充算法生成的数据包是否是开头和结束的标志
		buffer[24] = 0x1
		if shardIndex == 0 {
			buffer[24] = 0x5
		}
		if shardIndex == dataShards-1 {
			buffer[24] = 0x3
		}
		return buffer
	}
}

//func ReadVideoPacket(data []byte, packet *VideoPacket) error {
//	if packet == nil {
//		return errors.New("传入的packet不允许为空！")
//	}
//	if len(data) < VideoHeaderLength {
//		return errors.New("数据长度不足")
//	}
//	// 1. RTP Header 解析
//	packet.Header.Rtp.Header = data[0]
//	packet.Header.Rtp.PacketType = data[1]
//	packet.Header.Rtp.SequenceNumber = binary.BigEndian.Uint16(data[2:4])
//	packet.Header.Rtp.Timestamp = binary.BigEndian.Uint32(data[4:8])
//	packet.Header.Rtp.Ssrc = binary.BigEndian.Uint32(data[8:12])
//	// 2. Reserved 填充 (内联直接赋值，避免数组转型)
//	packet.Header.Reserved[0] = data[12]
//	packet.Header.Reserved[1] = data[13]
//	packet.Header.Reserved[2] = data[14]
//	packet.Header.Reserved[3] = data[15]
//
//	// 3. NV_VIDEO_PACKET 解析
//	packet.Header.Packet.StreamPacketIndex = binary.BigEndian.Uint32(data[16:20])
//	packet.Header.Packet.FrameIndex = binary.BigEndian.Uint32(data[20:24])
//	packet.Header.Packet.Flags = data[24]
//	packet.Header.Packet.ExtraFlags = data[25]
//	packet.Header.Packet.MultiFecFlags = data[26]
//	packet.Header.Packet.MultiFecBlocks = data[27]
//	packet.Header.Packet.FecInfo = binary.BigEndian.Uint32(data[28:32])
//	// 4. Payload 零拷贝引用（切片指针重定向）
//	packet.Payload = data[VideoHeaderLength:]
//	return nil
//}
//
//func WriteVideoPacket(packet *VideoPacket, buffer []byte) ([]byte, error) {
//	//binary.Write() 也可以用binary.Write，但据说性能很低
//	total := VideoHeaderLength + len(packet.Payload)
//	if buffer == nil {
//		buffer = make([]byte, total)
//	} else {
//		if cap(buffer) < total {
//			return nil, errors.New("缓冲长度不足！")
//		}
//		buffer = buffer[:total]
//	}
//	buffer[0] = packet.Header.Rtp.Header
//	buffer[1] = packet.Header.Rtp.PacketType
//	binary.BigEndian.PutUint16(buffer[2:4], packet.Header.Rtp.SequenceNumber)
//	binary.BigEndian.PutUint32(buffer[4:8], packet.Header.Rtp.Timestamp)
//	binary.BigEndian.PutUint32(buffer[8:12], packet.Header.Rtp.Ssrc)
//	buffer[12] = packet.Header.Reserved[0]
//	buffer[13] = packet.Header.Reserved[1]
//	buffer[14] = packet.Header.Reserved[2]
//	buffer[15] = packet.Header.Reserved[3]
//	binary.BigEndian.PutUint32(buffer[16:20], packet.Header.Packet.StreamPacketIndex)
//	binary.BigEndian.PutUint32(buffer[20:24], packet.Header.Packet.FrameIndex)
//	buffer[24] = packet.Header.Packet.Flags
//	buffer[25] = packet.Header.Packet.ExtraFlags
//	buffer[26] = packet.Header.Packet.MultiFecFlags
//	buffer[27] = packet.Header.Packet.MultiFecBlocks
//	binary.BigEndian.PutUint32(buffer[28:VideoHeaderLength], packet.Header.Packet.FecInfo)
//	copy(buffer[VideoHeaderLength:], packet.Payload) //Payload 拷贝 (High-performance memmove)
//	return buffer, nil
//}
