package network

import (
	"encoding/binary"
	"errors"
	"unsafe"
)

type RtpPacket struct {
	Header         uint8
	PacketType     uint8
	sequenceNumber uint16
	timestamp      uint32
	ssrc           uint32
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

// ConvertByteToVideoPacket 将数据直接转成结构，要求数据结构对齐，修改结构变量即修改数据变量，这里会默认执行Little-Endian
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

func ReadVideoPacket(data []byte, packet *VideoPacket) error {
	if packet == nil {
		return errors.New("传入的packet不允许为空！")
	}
	if len(data) < VideoHeaderLength {
		return errors.New("数据长度不足")
	}
	// 1. RTP Header 解析
	packet.Header.Rtp.Header = data[0]
	packet.Header.Rtp.PacketType = data[1]
	packet.Header.Rtp.sequenceNumber = binary.BigEndian.Uint16(data[2:4])
	packet.Header.Rtp.timestamp = binary.BigEndian.Uint32(data[4:8])
	packet.Header.Rtp.ssrc = binary.BigEndian.Uint32(data[8:12])
	// 2. Reserved 填充 (内联直接赋值，避免数组转型)
	packet.Header.Reserved[0] = data[12]
	packet.Header.Reserved[1] = data[13]
	packet.Header.Reserved[2] = data[14]
	packet.Header.Reserved[3] = data[15]

	// 3. NV_VIDEO_PACKET 解析
	packet.Header.Packet.StreamPacketIndex = binary.BigEndian.Uint32(data[16:20])
	packet.Header.Packet.FrameIndex = binary.BigEndian.Uint32(data[20:24])
	packet.Header.Packet.Flags = data[24]
	packet.Header.Packet.ExtraFlags = data[25]
	packet.Header.Packet.MultiFecFlags = data[26]
	packet.Header.Packet.MultiFecBlocks = data[27]
	packet.Header.Packet.FecInfo = binary.BigEndian.Uint32(data[28:32])
	// 4. Payload 零拷贝引用（切片指针重定向）
	packet.Payload = data[VideoHeaderLength:]
	return nil
}

func WriteVideoPacket(packet *VideoPacket, buffer []byte) ([]byte, error) {
	//binary.Write() 也可以用binary.Write，但据说性能很低
	total := VideoHeaderLength + len(packet.Payload)
	if buffer == nil {
		buffer = make([]byte, total)
	} else {
		if cap(buffer) < total {
			return nil, errors.New("缓冲长度不足！")
		}
		buffer = buffer[:total]
	}
	buffer[0] = packet.Header.Rtp.Header
	buffer[1] = packet.Header.Rtp.PacketType
	binary.BigEndian.PutUint16(buffer[2:4], packet.Header.Rtp.sequenceNumber)
	binary.BigEndian.PutUint32(buffer[4:8], packet.Header.Rtp.timestamp)
	binary.BigEndian.PutUint32(buffer[8:12], packet.Header.Rtp.ssrc)
	buffer[12] = packet.Header.Reserved[0]
	buffer[13] = packet.Header.Reserved[1]
	buffer[14] = packet.Header.Reserved[2]
	buffer[15] = packet.Header.Reserved[3]
	binary.BigEndian.PutUint32(buffer[16:20], packet.Header.Packet.StreamPacketIndex)
	binary.BigEndian.PutUint32(buffer[20:24], packet.Header.Packet.FrameIndex)
	buffer[24] = packet.Header.Packet.Flags
	buffer[25] = packet.Header.Packet.ExtraFlags
	buffer[26] = packet.Header.Packet.MultiFecFlags
	buffer[27] = packet.Header.Packet.MultiFecBlocks
	binary.BigEndian.PutUint32(buffer[28:VideoHeaderLength], packet.Header.Packet.FecInfo)
	copy(buffer[VideoHeaderLength:], packet.Payload) //Payload 拷贝 (High-performance memmove)
	return buffer, nil
}
