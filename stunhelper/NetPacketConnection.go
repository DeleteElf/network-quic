package stunhelper

import (
	"github.com/pion/stun/v3"
	"net"
)

// NetPacketConnection 将 net.PacketConn 包装为同时支持 io.Reader 和 io.Writer 的对象
type NetPacketConnection struct {
	conn   net.PacketConn
	target net.Addr
}

func NewNetPacketConnection(pConn net.PacketConn, targetAddr net.Addr) stun.Connection {
	return &NetPacketConnection{
		conn:   pConn,
		target: targetAddr,
	}
}

// Write 实现 io.Writer 接口：把数据通过 WriteTo 发送到指定的 STUN 服务器
func (a *NetPacketConnection) Write(p []byte) (n int, err error) {
	return a.conn.WriteTo(p, a.target)
}

// Read 实现 io.Reader 接口：从 PacketConn 读取数据（自动丢弃对端地址信息）
func (a *NetPacketConnection) Read(p []byte) (n int, err error) {
	n, _, err = a.conn.ReadFrom(p)
	return n, err
}

func (a *NetPacketConnection) Close() error {
	//if a.conn != nil {
	//	err := a.conn.Close()
	//	a.conn = nil
	//	return err
	//}
	return nil
}
