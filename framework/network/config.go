package network

import (
	"github.com/quic-go/quic-go"
)

const (
	FecPacketHeaderLength = 19
	FecLimitPacketSize    = 100
	NetMtuPacketSize      = 1400
)

type StreamConfig struct {
	Type         StreamType
	EnableFec    bool
	DataShards   int
	ParityShards int
}

type Config struct {
	SupportFec    bool
	QuicConfig    *quic.Config
	MtuPacketSize uint16
}
