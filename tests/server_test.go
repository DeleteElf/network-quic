package tests

import (
	"fmt"
	"github.com/DeleteElf/zero-net/framework/network"
	"github.com/DeleteElf/zero-net/framework/utils"
	"github.com/DeleteElf/zero-net/server"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
	"log/slog"
	"strconv"
	"testing"
	"time"
)

var restart bool = false

func socketHandler(svr *server.Server, sock *network.Socket) {
	if sock == nil {
		slog.Error("客户端已经不存在！")
		return
	}
	for i := 0; i < sock.ChannelCount; i++ {
		go messageHandler(svr, sock, i)
	}
}

func messageHandler(svr *server.Server, sock *network.Socket, channelIndex int) {
	for {
		if sock.IsClosed {
			break
		}
		_, err := sock.ReceiveDataToBuffer(channelIndex) //这个会卡住等待
		if err != nil {
			slog.Error(err.Error())
			break
		}
		if sock.IsClosed {
			break
		}
		if sock.StreamChannels[channelIndex] == nil {
			break
		}
		currentBuffer := sock.StreamChannels[channelIndex].Buffer
		if currentBuffer == nil {
			break
		}
		sock.StreamChannels[channelIndex].Buffer = nil
		msg := string(currentBuffer.Data)
		slog.Debug("收到数据：", slog.Int("channelId", currentBuffer.ChannelId), slog.String("msg", msg),
			slog.String("clientId", currentBuffer.ClientId))
		if msg == "hello,如果数据太短，我们在fec模式下，就会报错，谨记！！！" {
			template := "这是一条测试数据，我们需要用来测试一下 fec的功能是否正常，因此，我们会不断地评价它！！！"
			stringList := make([]string, 10)
			result := ""
			for index := 0; index < 10; index++ {
				result = result + template
				stringList[index] = result
				temp := strconv.Itoa(index) + ":" + result
				data := []byte(temp)
				slog.Debug("正在向客户端发送fec数据包", slog.Int("channelId", currentBuffer.ChannelId),
					slog.Int("数据长度:", len(data)), slog.String("msg", temp))
				_, err = sock.Send(channelIndex, data)
				if err != nil {
					return
				}
			}
			for i := 9; i >= 0; i-- {
				temp := strconv.Itoa(i+10) + ":" + stringList[i]
				data := []byte(temp)
				slog.Debug("正在向客户端发送fec数据包", slog.Int("channelId", currentBuffer.ChannelId),
					slog.Int("数据长度:", len(data)), slog.String("msg", temp))
				if svr.QuicConfig.EnableDatagrams {
					_, err = sock.SendFecDatagram(channelIndex, uint64(i+10), data) //这里我们模拟强制乱序，接收重组
				} else {
					_, err = sock.Send(channelIndex, data)
				}
				if err != nil {
					return
				}
			}
		} else if msg == "bye" {
			slog.Debug("收到结束会话命令！")
			_ = testServer.CloseSocket(sock.Id)
		} else if msg == "shutdown" {
			restart = false
			slog.Debug("收到关闭命令！")
			testServer.Close()
			return
			//} else if msg == "restart" {
			//	slog.Debug("收到重启命令！")
			//	restart = true
			//	svr.Close()
		} else {
			returnMsg := fmt.Sprintf("收到数据来自[%d]的数据：%s", currentBuffer.ChannelId, msg)
			_, err2 := sock.Send(currentBuffer.ChannelId, []byte(returnMsg))
			if err2 != nil {
				slog.Error(err2.Error())
			}
		}
	}
}

var testServer *server.Server

func TestServer(t *testing.T) {
	utils.InitLog(slog.LevelDebug, nil)                     //初始化日志
	testServer = server.NewServerByAddress("0.0.0.0:10001") //尝试连接本机服务
	testServer.SupportFec = true
	for {
		if restart {
			time.Sleep(1 * time.Second)
			slog.Debug("服务端重新启动监听！")
			restart = false
		}
		testServer.OnAcceptSocket = func(sock *network.Socket) {
			slog.Debug("新的客户端接入：", slog.String("id", sock.Id))
			go socketHandler(testServer, sock)
		}
		testServer.QuicConfig = &quic.Config{
			// MaxIncomingStreams: 0xffffffffffff, // 最大默认stream输入，默认100
			HandshakeIdleTimeout:    5 * time.Second,         // 默认5s
			MaxIdleTimeout:          10 * time.Second,        // 默认30s
			KeepAlivePeriod:         3 * time.Second,         // 建议是 MaxIdleTimeout 的一半，或者更小的值
			InitialPacketSize:       testServer.FecBlockSize, //初始包大小
			DisablePathMTUDiscovery: false,                   // 允许路径 MTU 探索
			Allow0RTT:               true,
			EnableDatagrams:         testServer.SupportFec, //允许直接传输udp
			Tracer:                  qlog.DefaultConnectionTracer,
		}
		testServer.StartListen(func(sock *network.Socket) {
			slog.Debug("客户端断开连接：", slog.String("id", sock.Id))
		})
		slog.Debug("服务端退出监听！")
		if !restart {
			break
		}
	}
}
