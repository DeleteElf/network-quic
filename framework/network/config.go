package network

import (
	"github.com/quic-go/quic-go"
)

type StreamConfig struct {
	Type         StreamType
	EnableFec    bool
	DataShards   int
	ParityShards int
}

type Config struct {
	SupportFec bool
	QuicConfig *quic.Config
}
