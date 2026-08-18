package agent

import (
	"fmt"
	"github.com/pkg/errors"
	"io"
	"net"
	"time"
)

// Socket 用于包装代理需要的头，实现了 net.PacketConn的所有方法，可以被用于quic的监听
type Socket struct {
	Conn      net.PacketConn
	ReadData  []byte
	WriteData []byte
	Idx       uint32

	io.Closer
}

func (a *Socket) ReadFrom(p []byte) (int, net.Addr, error) {
	if a.Conn == nil {
		return 0, nil, errors.New("agent socket`s conn is null")
	}
	dataLen, addr, err := a.Conn.ReadFrom(a.ReadData)
	if dataLen <= AGENT_HEAD_SIZE {
		return 0, addr, err
	}
	readLen := dataLen - AGENT_HEAD_SIZE
	copy(p[0:readLen], a.ReadData[AGENT_HEAD_SIZE:dataLen])
	return readLen, addr, err
}

func (a *Socket) WriteTo(p []byte, addr net.Addr) (int, error) {
	bufLen := AGENT_HEAD_SIZE + len(p)
	if bufLen > AGENT_PKG_SIZE {
		return -1, fmt.Errorf("too large")
	}
	copy(a.WriteData[AGENT_HEAD_SIZE:bufLen], p)
	if a.Conn == nil {
		return 0, errors.New("agent socket`s conn is null")
	}
	writeLen, err := a.Conn.WriteTo(a.WriteData[0:bufLen], addr)
	if writeLen < AGENT_HEAD_SIZE {
		return writeLen, err
	}
	return writeLen - AGENT_HEAD_SIZE, err
}

func (a *Socket) Close() error {
	a.Conn = nil
	return nil
}

func (a *Socket) LocalAddr() net.Addr {
	if a.Conn != nil {
		return a.Conn.LocalAddr()
	}
	return nil
}

func (a *Socket) SetDeadline(t time.Time) error {
	if a.Conn != nil {
		return a.Conn.SetDeadline(t)
	}
	return nil
}

func (a *Socket) SetReadDeadline(t time.Time) error {
	if a.Conn != nil {
		return a.Conn.SetReadDeadline(t)
	}
	return nil
}

func (a *Socket) SetWriteDeadline(t time.Time) error {
	if a.Conn != nil {
		return a.Conn.SetWriteDeadline(t)
	}
	return nil
}
