package tests

import (
	"github.com/DeleteElf/zero-net/framework/utils"
	"github.com/DeleteElf/zero-net/ice"
	"log/slog"
	"testing"
)

func TestStunClient(t *testing.T) {
	utils.InitLog(slog.LevelDebug, nil)
	client := ice.NewStunClient()
	//err := client.Connect("stun:192.168.199.22:3478", "test")
	//err := client.Connect("stun:121.41.228.111:3478", "test")
	err := client.Connect("stun:stun.new0.com.cn:3478", "test", nil)
	//err := client.Connect("stun:127.0.0.1:3478")
	if err != nil {
		slog.Error("连接stun服务时发生错误！", slog.Any("err", err))
	} else {
		slog.Info("====================================")
		slog.Info("你的公网 IP 地址 :", slog.Any("ip", client.ExternalAddress.IP))
		slog.Info("你的公网映射端口 : ", slog.Int("port", client.ExternalAddress.Port))
		slog.Info("====================================")
	}
	client.Close()
}
