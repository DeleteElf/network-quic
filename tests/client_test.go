package tests

import (
	"github.com/DeleteElf/zero-net/client"
	"github.com/DeleteElf/zero-net/framework/network"
	"github.com/DeleteElf/zero-net/framework/utils"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
	"log/slog"
	"testing"
	"time"
)

func receiveHandler(cli *client.Client, channelIndex int) {
	for {
		if cli.IsClosed || cli.Socket.IsClosed {
			break
		}
		slog.Info("正在准备接收数据", slog.Int("channel", channelIndex))
		_, err := cli.Socket.ReceiveDataToBuffer(channelIndex)
		if err != nil {
			slog.Error("ReceiveDataToBuffer error", slog.Any("err", err))
			return
		}
		if channelIndex >= len(cli.Socket.StreamChannels) {
			return
		}
		buffer := cli.Socket.StreamChannels[channelIndex].Buffer
		if buffer != nil {
			slog.Info("收到来自服务端的新消息", slog.Int("channel", channelIndex), slog.String("msg", string(buffer.Data)))
			cli.Socket.StreamChannels[channelIndex].Buffer = nil
			if channelIndex == 0 {
				//_, _ = cli.Socket.Send(channelIndex, []byte("bye"))
				//slog.Info("send bye", slog.Int("channel", channelIndex))
				//cli.Close()
				//} else if channelIndex == 1 {
				//	//time.Sleep(500 * time.Millisecond)
				//	_, _ = cli.Send(cli.Streams[channelIndex], []byte("restart"))
			} else if channelIndex == 2 {
				//_, _ = cli.Socket.Send(channelIndex, []byte("shutdown"))
			}
		}
	}
}

func TestClient(t *testing.T) {
	utils.InitLog(slog.LevelDebug, nil)                       //初始化日志
	cli := client.NewClient("192.168.199.22:10001", "test01") //尝试连接本机服务
	cli.SupportFec = true
	cli.QuicConfig = &quic.Config{
		//MaxIncomingStreams:      0xffffffffffff,   // 最大默认stream输入，默认100
		HandshakeIdleTimeout:    5 * time.Second,          // 默认5s
		MaxIdleTimeout:          10 * time.Second,         // 默认30s，我们这边设置成10秒
		KeepAlivePeriod:         3 * time.Second,          // 建议是 MaxIdleTimeout 的一半，或者更小的值
		InitialPacketSize:       network.NetMtuPacketSize, //当前最大数据包一个基础包的大小
		DisablePathMTUDiscovery: false,
		Allow0RTT:               true,
		EnableDatagrams:         cli.SupportFec,
		Tracer:                  qlog.DefaultConnectionTracer,
	}
	cli.OnSocketConnected = func(sock *network.Socket) {
		if cli.SupportFec {
			sock.StreamConfigs[1].SetStreamType(network.StreamType(1)) //设置通道媒体类型
			sock.StreamConfigs[1].Type = network.StreamType(0)
		}
	}
	err := cli.Connect(3, network.STREAM_NETWORK_UDP, func(sock *network.Socket) {
		slog.Debug("socket已经断开===》！", slog.String("clientId", sock.Id))
	}) //创建udp网络

	if err != nil {
		slog.Error("客户端连接失败", slog.Any("err", err))
		return
	}
	slog.Info("客户端连接成功！", slog.Int("通道数", cli.Socket.ChannelCount))
	for i := 0; i < cli.Socket.ChannelCount; i++ {
		go receiveHandler(cli, i)
	}

	//msg1 := "hello,i am channel 1 data from client"
	//slog.Info("正在向通道1发送数据", slog.String("msg", msg1))
	//_, _ = cli.Socket.Send(1, []byte(msg1))

	msg2 := "hello,i am channel 2 data from client"
	slog.Info("正在向通道2发送数据", slog.String("msg", msg2))
	_, _ = cli.Socket.Send(2, []byte(msg2))

	msg3 := "hello,如果数据太短，我们在fec模式下，就会报错，谨记！！！"
	_, _ = cli.Socket.Send(1, []byte(msg3))
	slog.Info("正在向通道1发送数据", slog.String("msg", msg3))
	//time.Sleep(time.Second * 3) //等待3秒，等他们通讯完成再退出
	for {
		time.Sleep(time.Second * 6)
		if cli.IsClosed || cli.Socket == nil || cli.Socket.IsClosed {
			break
		} else {
			_, _ = cli.Socket.Ping(0)
		}
	}
}
