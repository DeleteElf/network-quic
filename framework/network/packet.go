package network

import (
	"encoding/binary"
	"errors"
	"unsafe"
)

const (
	FecPacketHeaderLength    = 18
	FecLimitPacketSize       = 100
	NetMtuPacketSize         = 1400
	VideoHeaderLength        = 32
	AudioHeaderLength        = 24
	RtpHeaderLength          = 12
	NvidiaPacketHeaderLength = 16
)

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
	Header  *VideoPacketHeader
	Payload []byte // 直接用 slice 存放后续的二进制数据
}

type AudioPacket struct {
	Rtp RtpPacket
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
}

// ConvertByteToRtpPacket 将数据直接转成RtpPacket结构，要求数据结构对齐，修改结构变量即修改数据变量，windows这里会默认执行Little-Endian
func ConvertByteToRtpPacket(data []byte) (*RtpPacket, error) {
	if len(data) >= RtpHeaderLength {
		return (*RtpPacket)(unsafe.Pointer(&data[0])), nil
	}
	return nil, errors.New("转换失败！")
}

func ConvertByteToAudioFecPacket(data []byte) (*AudioFecPacket, error) {
	if len(data) >= AudioHeaderLength {
		return (*AudioFecPacket)(unsafe.Pointer(&data[0])), nil
	}
	return nil, errors.New("转换失败！")
}

// ConvertByteToVideoPacket 将数据直接转成VideoPacket结构，要求数据结构对齐，修改结构变量即修改数据变量，windows这里会默认执行Little-Endian
func ConvertByteToVideoPacket(data []byte) (*VideoPacket, error) {
	if len(data) >= VideoHeaderLength {
		return &VideoPacket{
			// 1. 强转 Header：修改 pkt.Header.Rtp.Header 会直接修改 data[0]
			Header: (*VideoPacketHeader)(unsafe.Pointer(&data[0])),
			// 2. 截取 Payload：修改 pkt.Payload[0] 会直接修改 data[32]
			Payload: data[VideoHeaderLength:],
		}, nil
	}
	return nil, errors.New("转换失败！")
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
func RebuildRtpPacket(header, data []byte, shardIndex, dataShards uint16) []byte {
	if header[1] == 0x61 || header[1] == 0x7f {
		total := RtpHeaderLength + len(data)
		buffer := make([]byte, total)               //循环中，且释放时机不好确定，不使用sync.Pool
		copy(buffer[0:], header[0:RtpHeaderLength]) //仅拷贝加入rtp包即可
		copy(buffer[RtpHeaderLength:], data)
		switch header[1] {
		case 0x61: //标准音频
			sequenceNumber := binary.BigEndian.Uint16(buffer[2:])
			sequenceNumber = sequenceNumber - sequenceNumber%dataShards + shardIndex //计算当前的包
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
		sequenceNumber := binary.BigEndian.Uint16(buffer[2:]) - oldShardIndex + shardIndex
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
