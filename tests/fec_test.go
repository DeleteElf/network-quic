package tests

import (
	"fmt"
	"github.com/DeleteElf/zero-net/client"
	"github.com/DeleteElf/zero-net/framework/network"
	"github.com/DeleteElf/zero-net/framework/utils"
	"github.com/DeleteElf/zero-net/server"
	"github.com/klauspost/reedsolomon"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
	"log/slog"
	"math/bits"
	"strconv"
	"testing"
	"time"
)

func TestFecSimple(t *testing.T) {
	utils.InitLog(slog.LevelDebug, nil)
	encoder, _ := reedsolomon.New(4, 2)
	buffers := make([][]byte, 6)
	shards := make([][]byte, 6)
	for i := 0; i < 4; i++ {
		buffers[i] = []byte{byte(i + 1), byte(9 - i), byte(9 - i), byte(9 - i), byte(9 - i), byte(9 - i), byte(9 - i), byte(9 - i)}
		shards[i] = buffers[i][1:]
	}
	for i := 4; i < 6; i++ {
		buffers[i] = make([]byte, 8)
		shards[i] = buffers[i][1:]
	}
	encoder.Encode(shards)
	for i := 4; i < 6; i++ {
		buffers[i][0] = byte(i + 1)
	}
	result_shards := make([][]byte, 6)
	result_shards[0] = buffers[0][1:]
	result_shards[1] = buffers[1][1:]
	result_shards[3] = buffers[3][1:]
	result_shards[5] = buffers[5][1:]
	buffers[2] = nil
	buffers[4] = nil
	err := encoder.ReconstructData(result_shards)
	if err != nil {
		return
	}
	slog.Debug("1111")
}

func TestFecClient(t *testing.T) {
	utils.InitLog(slog.LevelDebug, nil)                       //初始化日志
	cli := client.NewClient("192.168.199.22:10001", "test01") //尝试连接本机服务
	cli.SupportFec = true
	cli.FecBlockSize = 1040
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
		for i := 0; i < sock.ChannelCount; i++ {
			sock.StreamConfigs[i].SetStreamType(network.StreamType(i)) //设置通道媒体类型
			sock.StreamConfigs[i].FecPacketSize = cli.FecBlockSize
			if i == 0 {
				sock.StreamConfigs[i].EnableFec = true
				sock.StreamConfigs[i].DataShards = 4
				sock.StreamConfigs[i].ParityShards = 2
			}
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
	msg2 := "hello"
	slog.Info("正在向通道2发送数据", slog.String("msg", msg2))
	for i := 0; i < 2; i++ {
		_, _ = cli.Socket.Send(i+1, []byte(msg2))
	}
	msg3 := "hello,如果数据太短，我们在fec模式下，就会报错，谨记！！！"
	_, _ = cli.Socket.Send(0, []byte(msg3))
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

func fecMessageHandler(sock *network.Socket, channelIndex int) {
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
		if msg == "hello" {
			if channelIndex == 1 { //模拟sunshine的音频核心业务逻辑
				time := uint32(0)
				buffer := make([]byte, 664)
				for j := 0; j < 5; j++ {
					sock.StreamChannels[1].FrameIndex = uint64(j)
					for i := 0; i < 4; i++ {
						rtp, _ := network.ConvertByteToRtpPacket(buffer)
						rtp.Header = 0x80
						rtp.PacketType = 0x0061
						rtp.SequenceNumber = bits.ReverseBytes16(uint16(j*4 + i))
						rtp.Timestamp = bits.ReverseBytes32(time)
						//network.RtpPacket{
						//	Header:         0x80,
						//	PacketType:     0x0061,
						//	SequenceNumber: bits.ReverseBytes16(1),
						//	Timestamp:      bits.ReverseBytes32(time),
						//	Ssrc:           0,
						//}
						time += 5
						sock.SendFecDatagram(channelIndex, false, buffer)
					}
				}

			} else if channelIndex == 2 { //模拟sunshine的核心逻辑，虽然我们并没有载体数据！
				blockSize := int(sock.StreamConfigs[channelIndex].FecPacketSize)
				fec_blocks_needed := 1
				blockIndex := 0
				data_shards := 10
				buffer := make([]byte, 10400)
				//shards := make([][]byte, data_shards)
				percentage := 30
				for i := 0; i < data_shards; i++ {
					buf := buffer[i*blockSize : (i+1)*blockSize]
					packet, _ := network.ConvertByteToVideoPacket(buf)
					packet.Header.Rtp.Header = 0x80
					packet.Header.Rtp.PacketType = 1
					packet.Header.Rtp.SequenceNumber = bits.ReverseBytes16(uint16(i))
					packet.Header.Rtp.Timestamp = bits.ReverseBytes32(uint32(time.Now().UnixMilli()))
					packet.Header.Rtp.Ssrc = bits.ReverseBytes32(0)

					packet.Header.Packet.StreamPacketIndex = 1 << 8
					packet.Header.Packet.FrameIndex = 1
					packet.Header.Packet.Flags = 0x1
					packet.Header.Packet.MultiFecFlags = 0x10
					packet.Header.Packet.MultiFecBlocks = uint8((blockIndex << 4) | ((fec_blocks_needed - 1) << 6))
					packet.Header.Packet.FecInfo = uint32(i<<12 | data_shards<<22 | percentage<<4)
					if i == 0 {
						packet.Header.Packet.Flags |= 0x4
					}
					if i == data_shards-1 {
						packet.Header.Packet.Flags |= 0x2
					}
					//_, _ = network.WriteVideoPacket(packet, buf)
					//shards[i] = buf
				}
				sock.Send(channelIndex, buffer)
			}
		} else if msg == "hello,如果数据太短，我们在fec模式下，就会报错，谨记！！！" {
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
				//if svr.QuicConfig.EnableDatagrams {
				//	_, err = sock.SendFecDatagram(channelIndex, uint64(i+10), false, data) //这里我们模拟强制乱序，接收重组
				//} else {
				_, err = sock.Send(channelIndex, data)
				//}
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

func TestFecServer(t *testing.T) {
	utils.InitLog(slog.LevelDebug, nil)                     //初始化日志
	testServer = server.NewServerByAddress("0.0.0.0:10001") //尝试连接本机服务
	testServer.SupportFec = true
	testServer.FecBlockSize = 1040 //设置fec的传输块大小
	for {
		if restart {
			time.Sleep(1 * time.Second)
			slog.Debug("服务端重新启动监听！")
			restart = false
		}
		testServer.OnAcceptSocket = func(sock *network.Socket) {
			slog.Debug("新的客户端接入：", slog.String("id", sock.Id))
			for i := 0; i < sock.ChannelCount; i++ {
				if i != 0 {
					sock.StreamConfigs[i].SetStreamType(network.StreamType(i)) //设置通道媒体类型
				} else {
					sock.StreamConfigs[i].SetStreamType(network.StreamType(2)) //强制启动文本的fec
					sock.StreamConfigs[i].Type = network.StreamType(i)
				}
				sock.StreamConfigs[i].FecPacketSize = testServer.FecBlockSize
				go fecMessageHandler(sock, i)
			}
		}
		testServer.QuicConfig = &quic.Config{
			// MaxIncomingStreams: 0xffffffffffff, // 最大默认stream输入，默认100
			HandshakeIdleTimeout:    5 * time.Second,          // 默认5s
			MaxIdleTimeout:          10 * time.Second,         // 默认30s
			KeepAlivePeriod:         3 * time.Second,          // 建议是 MaxIdleTimeout 的一半，或者更小的值
			InitialPacketSize:       network.NetMtuPacketSize, //初始包大小
			DisablePathMTUDiscovery: false,                    // 允许路径 MTU 探索
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
