package ice

import (
	"github.com/quic-go/quic-go"
	"log/slog"
	"net"
	"strings"
	"time"
)

type ConnectionState int

const (
	None ConnectionState = iota
	Connecting
	Connected
	Closing
)

type IceObject struct {
	SessionId string
	Ip        string
	Port      int
	State     ConnectionState
}

type IceWorker struct {
	Stun       []string
	NetConn    net.PacketConn
	IceChannel chan IceObject
	QuicConn   *quic.Conn
	IsInQuic   bool
}

// DetectStun 探测stun服务获取公网ip和端口
func (iw *IceWorker) DetectStun(token string) (ip string, port int) {
	if len(iw.Stun) > 0 {
		for i, s := range iw.Stun {
			slog.Debug("配置了stun服务，正在准备探测！", slog.Int("序号", i), slog.String("address", s))
			cli := NewStunClient()
			err := cli.Connect(s, token, iw.NetConn)
			if err == nil {
				if len(ip) > 0 && ip != cli.ExternalAddress.IP.String() {
					slog.Warn("你的公网 IP 地址发生变化 :", slog.String("ip", ip), slog.String("newip", cli.ExternalAddress.IP.String()))
				} else if len(ip) == 0 && len(cli.ExternalAddress.IP.String()) > 0 {
					ip = cli.ExternalAddress.IP.String()
				}
				if port > 0 && port != cli.ExternalAddress.Port {
					slog.Warn("你的公网 IP 端口发生变化 :", slog.Int("port", port), slog.Int("newport", cli.ExternalAddress.Port))
				} else if port == 0 && cli.ExternalAddress.Port > 0 {
					port = cli.ExternalAddress.Port
				}
			}
		}
		slog.Debug("你的公网 IP 地址 :", slog.Any("ip", ip))
		slog.Debug("你的公网映射端口 : ", slog.Int("port", port))
	}
	return ip, port
}

// PunchHoleAsync 提供服务端打洞的函数
func (iw *IceWorker) PunchHoleAsync(targetAddr net.Addr, message string) error {
	if iw.IceChannel == nil {
		slog.Warn("请先构建打洞成功的通道！")
		return nil
	}
	slog.Info("服务端：开始异步向客户端盲发 UDP 包冲刷 NAT 洞口...", slog.String("target", targetAddr.String()))
	go func() {
		for i := 0; i < 10; i++ { //网上说打动需要多次冲刷，但实际测试就1次就可以了
			_, _ = iw.NetConn.WriteTo([]byte(message), targetAddr)
			time.Sleep(20 * time.Millisecond)
		}
		slog.Debug("服务端：NAT 出口首次冲刷完成！")
	}()
	go func() {
		conn := iw.NetConn
		buf := make([]byte, 1024)
		//isFirst := true
		//quicConn := iw.QuicConn
		for {
			//if quicConn != nil { //因为没有实际连接，实际上我们不用处理这个逻辑
			//	data, err := quicConn.ReceiveDatagram(context.Background())
			//	if err != nil {
			//		slog.Debug("客户端打洞超时/失败: %w", err)
			//	}
			//	recvStr := string(data)
			//	ips := strings.Split(quicConn.RemoteAddr().String(), ":")
			//	port, _ := strconv.Atoi(ips[1])
			//	if recvStr == message && len(ips) == 2 { //为了防止污染数据，我们需要校验一下消息内容
			//		iw.IceChannel <- IceObject{
			//			SessionId: message,
			//			Ip:        quicConn.RemoteAddr().String(),
			//			Port:      port,
			//			State:     Connected,
			//		}
			//		return
			//	}
			//} else {
			n, addr, err := conn.ReadFrom(buf)
			if err != nil {
				slog.Debug("服务端打洞超时/失败:", slog.Any("err", err))
				continue
			}
			//if isFirst {
			//	go func() { //再连续发10次
			//		for i := 0; i < 10; i++ { //网上说打动需要多次冲刷，但实际测试就1次就可以了
			//			_, _ = iw.NetConn.WriteTo([]byte(message), targetAddr)
			//			time.Sleep(20 * time.Millisecond)
			//		}
			//	}()
			//	isFirst = false
			//}
			if n == 0 && iw.IsInQuic { //如果在quic模式下，因为只能收到空字符串，我们这边简化一下
				//iw.IceChannel <- IceObject{
				//	SessionId: message,
				//	State:     Connected,
				//}
				return
			} else {
				recvStr := string(buf[:n])
				if addr != nil {
					slog.Debug("服务端收到客户端的打洞包", slog.String("from", addr.String()), slog.String("data", recvStr))
					ips := strings.Split(addr.String(), ":")
					//port, _ := strconv.Atoi(ips[1])
					// 匹配来自服务端明确的冰打洞包
					if recvStr == message && len(ips) == 2 { //为了防止污染数据，我们需要校验一下消息内容
						if addr.String() != targetAddr.String() {
							slog.Warn("服务端收到打洞包与客户端提供的ip端口不一致，尝试使用此地址进行打洞发回消息", slog.String("from", addr.String()), slog.String("data", recvStr))
							//for i := 0; i < 10; i++ { //网上说打动需要多次冲刷，但实际测试就1次就可以了
							_, _ = iw.NetConn.WriteTo([]byte(message), addr)
							//time.Sleep(20 * time.Millisecond)
							//}
							//return
						} else {
							_, _ = iw.NetConn.WriteTo([]byte(message), targetAddr)
						}

						//iw.IceChannel <- IceObject{
						//	SessionId: message,
						//	Ip:        ips[0],
						//	Port:      port,
						//	State:     Connected,
						//}
						//return
					}
				}
			}
		}
	}()
	return nil
}

// PunchHole 【客户端打洞】：持续向服务端发包开洞，并等待服务端的回应
func (iw *IceWorker) PunchHole(targetAddr net.Addr, message string, timeout time.Duration, stopChannel chan struct{}) {
	slog.Info("客户端：开始与目标服务器进行 UDP 双向打洞...", slog.String("target", targetAddr.String()))
	conn := iw.NetConn
	//_ = conn.SetReadDeadline(time.Now().Add(timeout))
	//defer conn.SetReadDeadline(time.Time{})
	// 1. 后台持续给服务端发包，保持客户端 NAT 洞口开启
	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopChannel:
				return
			case <-ticker.C:
				_, _ = conn.WriteTo([]byte(message), targetAddr)
			}
		}
	}()
	buf := make([]byte, 1024)
	// 2. 阻塞接收服务端的打洞包
	for {
		n, addr, err := conn.ReadFrom(buf)
		if err != nil {
			//close(stopChannel)
			slog.Info("客户端打洞超时/失败:", slog.Any("err", err))
			continue
		}

		recvStr := string(buf[:n])
		if addr != nil {
			slog.Debug("客户端收到打洞回包", slog.String("from", addr.String()), slog.String("data", recvStr))

			// 匹配来自服务端明确的冰打洞包
			//if strings.Contains(recvStr, "ice-certification") {
			if recvStr == message {
				slog.Info("🎉 UDP 双向打洞成功！洞口已建立，准备发起 QUIC 握手")
				close(stopChannel)
				return
			}
		} else {
			//偶尔会收到addr 为nil的包，初步判断是quic包，待进一步验证
			slog.Debug("客户端收到打洞回包，无有效地址！", slog.String("data", recvStr))
		}
	}
}
