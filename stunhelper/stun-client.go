package stunhelper

import (
	"log/slog"
	"net"
)

type StunClient struct {
	Stun    []string
	NetConn net.PacketConn
}

// DetectStun 探测stun服务获取公网ip和端口
func (sc *StunClient) DetectStun(token string) (ip string, port int) {
	if len(sc.Stun) > 0 {
		for i, s := range sc.Stun {
			slog.Debug("配置了stun服务，正在准备探测！", slog.Int("序号", i), slog.String("address", s))
			cli := NewClient()
			err := cli.Connect(s, token, sc.NetConn)
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
