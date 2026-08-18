package network

import (
	"sync/atomic"
	"time"
)

type Status struct {
	//control *Control

	sentPkgs   atomic.Int64
	sentLens   atomic.Int64
	recvPkgs   atomic.Int64
	recvLens   atomic.Int64
	lostPkgs   atomic.Int64
	lastDrop   time.Time
	lastUpdate time.Time
}
