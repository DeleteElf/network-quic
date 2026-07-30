package tests

import (
	"github.com/DeleteElf/zero-net/framework/utils"
	"github.com/DeleteElf/zero-net/websocket"
	"github.com/deleteelf/goframework/utils/jsonhelper"
	"log/slog"
	"testing"
	"time"
)

func TestWebSocketClient(t *testing.T) {
	utils.InitLog(slog.LevelDebug, nil)
	client := websocket.NewClient()
	client.HeartTimeout = time.Second * 5
	isFirst := true
	client.OnConnected = func(address string) {
		slog.Info("与服务端连接", slog.String("address", address))
	}
	client.OnDisconnected = func(reason string) {
		slog.Info("与服务端断开连接", slog.String("reason", reason))
		result, _ := jsonhelper.GetJsonObject([]byte(reason))

		if client.Reconnect && result["code"].(float64) == 1 {
			_ = client.Connect(client.Address, client.HeartMessage)
		}
	}
	client.OnMessage = func(msg string) {
		slog.Info("收到新的消息", slog.String("msg", msg))
		if isFirst {
			isFirst = false
			for {
				time.Sleep(1 * time.Second) //模拟不处理，一直卡着
			}
		}
		slog.Info("处理完消息", slog.String("msg", msg))
	}
	go func() {
		err := client.Connect("wss://192.168.199.159:3005/device?type=device&apikey=575D6618206A2754", websocket.DefaultHeartMessage)
		if err != nil {
			slog.Error("连接发生错误", slog.Any("err", err))
		}
	}()
	stopTime := time.Now().Add(2 * time.Minute)
	for {
		time.Sleep(1 * time.Second)
		if time.Now().Compare(stopTime) > 0 {
			break
		}
	}
	client.Close()
}
