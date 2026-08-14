package ice

import (
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"
)

type IceWorker struct {
	Stun    []string
	NetConn net.PacketConn
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
	slog.Info("客户端：开始异步向服务端盲发 UDP 包冲刷 NAT 洞口...", slog.String("target", targetAddr.String()))
	go func() {
		pingMsg := []byte(message)
		for i := 0; i < 10; i++ {
			_, _ = iw.NetConn.WriteTo(pingMsg, targetAddr)
			time.Sleep(20 * time.Millisecond)
		}
		slog.Debug("客户端：NAT 出口冲刷完成！")
	}()
	return nil
}

// PunchHole 【客户端打洞】：持续向服务端发包开洞，并等待服务端的回应
func (iw *IceWorker) PunchHole(targetAddr net.Addr, message string, timeout time.Duration) error {
	slog.Info("客户端：开始与目标服务器进行 UDP 双向打洞...", slog.String("target", targetAddr.String()))
	conn := iw.NetConn
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	defer conn.SetReadDeadline(time.Time{})

	pingMsg := []byte(message)
	buf := make([]byte, 1024)
	stopChan := make(chan struct{})

	// 1. 后台持续给服务端发包，保持客户端 NAT 洞口开启
	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopChan:
				return
			case <-ticker.C:
				_, _ = conn.WriteTo(pingMsg, targetAddr)
			}
		}
	}()

	// 2. 阻塞接收服务端的打洞包
	for {
		n, addr, err := conn.ReadFrom(buf)
		if err != nil {
			close(stopChan)
			return fmt.Errorf("客户端打洞超时/失败: %w", err)
		}

		recvStr := string(buf[:n])
		slog.Debug("客户端收到打洞回包", slog.String("from", addr.String()), slog.String("data", recvStr))

		// 匹配来自服务端明确的冰打洞包
		if strings.Contains(recvStr, "ice-certification") {
			slog.Info("🎉 UDP 双向打洞成功！洞口已建立，准备发起 QUIC 握手")
			close(stopChan)
			return nil
		}
	}
}
