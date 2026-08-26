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
	//当前流的数据业务类型
	Type StreamType
	//是否启用Fec功能
	EnableFec bool
	//数据分片，仅当上层不是标准rtp数据包时，因没有提供fecInfo才用得到，否则优先考虑rtp包内协议
	DataShards int
	//奇偶碎片，仅当上层不是标准rtp数据包时，因没有提供fecInfo才用得到，否则优先考虑rtp包内协议
	ParityShards int

	FecPacketSize uint16
}

type Config struct {
	SupportFec bool
	//fec数据包的分块大小，数据包数据大小，不能大于这个数值
	FecBlockSize uint16
	//fec数据包的比例，如果包大小不够，则会是0
	//FecPercentage int
	//fec数据包的最小大小，小于这个值就不再执行fec
	FecLimitPacketSize int
	//如果执行fec，fec在总数量中的最小数量，默认是2，当FecPercentage==0时，不校验此参数
	FecMinRequiredPackets int
	//quic的配置
	QuicConfig *quic.Config
}

func (c *StreamConfig) SetStreamType(t StreamType) {
	if c.Type != t {
		c.Type = t
		if c.Type != Message {
			c.EnableFec = true
			switch c.Type {
			case Audio: //音频，默认33%
				c.DataShards = 4
				c.ParityShards = 2
			case Video: //视频，默认30%
				c.DataShards = 10
				c.ParityShards = 3
				break
			default:
				break
			}
		} else {
			c.EnableFec = false
		}
	}
}
