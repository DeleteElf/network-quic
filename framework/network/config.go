package network

import (
	"github.com/quic-go/quic-go"
)

const (
	FecPacketHeaderLength    = 19
	FecLimitPacketSize       = 100
	NetMtuPacketSize         = 1400
	VideoHeaderLength        = 32
	RtpHeaderLength          = 12
	NvidiaPacketHeaderLength = 16
)

type StreamConfig struct {
	Type         StreamType
	EnableFec    bool
	DataShards   int
	ParityShards int
}

type Config struct {
	SupportFec bool
	//fec数据包的分块大小，数据包数据大小，不能大于这个数值
	FecBlockSize int
	//fec数据包的比例，如果包大小不够，则会是0
	//FecPercentage int
	//fec数据包的最小大小，小于这个值就不再执行fec
	FecLimitPacketSize int
	//如果执行fec，fec在总数量中的最小数量，默认是2，当FecPercentage==0时，不校验此参数
	FecMinRequiredPackets int
	QuicConfig            *quic.Config
}
