package tests

import (
	"fmt"
	"github.com/DeleteElf/zero-net/agent"
	"github.com/DeleteElf/zero-net/client"
	"github.com/DeleteElf/zero-net/framework/network"
	"github.com/DeleteElf/zero-net/framework/utils"
	"log/slog"
	"strconv"
	"testing"
	"time"
)

func agentSocketHandler(sock *network.Socket) {
	if sock == nil {
		slog.Error("客户端已经不存在！")
		return
	}
	for i := 0; i < sock.ChannelCount; i++ {
		go angetMessageHandler(sock, i)
	}
}

func angetMessageHandler(sock *network.Socket, channelIndex int) {
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
		currentBuffer := sock.StreamChannels[channelIndex].Buffer
		if currentBuffer == nil {
			break
		}
		sock.StreamChannels[channelIndex].Buffer = nil
		msg := string(currentBuffer.Data)
		slog.Debug("收到数据：", slog.Int("channelId", currentBuffer.ChannelId), slog.String("msg", msg),
			slog.String("clientId", currentBuffer.ClientId))
		if msg == "bye" {
			slog.Debug("收到结束会话命令！")
			svr := testMgr.GetServer(sock.Id)
			if svr != nil {
				_ = svr.CloseSocket(sock.Id)
			}

		} else if msg == "shutdown" {
			restart = false
			slog.Debug("收到关闭命令！")
			svr := testMgr.GetServer(sock.Id)
			if svr != nil {
				svr.Close()
			}
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

var testMgr *agent.ManagePlatform

func TestHostAgent(t *testing.T) {
	utils.InitLog(slog.LevelDebug, nil)
	cfg := &agent.Config{
		MgrAddr: "wss://192.168.199.159:3005/device?type=proxy&apikey=575D6618206A2754",
		Hearts:  50,
		Data: utils.JsonObject(map[string]interface{}{
			"device_id": "0A76DE8C-1AB1-35C3-A137-FC9E10B1EF9F",
		}),
		//Port: 48000,
		Version:  "1",
		SignSalt: "2fbbdf99eae1675484a48e8310db1ee42d3bd6fdbc5e3f3755af848b23cc9817",
	}
	testMgr = agent.NewManagePlatform(cfg)
	testMgr.ConnectToPlatform()
	go testMgr.Hearts()
	for {
		if testMgr.IsClosed { //如果服务已经关闭，则不再继续连接管理平台
			break
		}
		if err := testMgr.ListenAgentConnect(func(sock *network.Socket) {
			slog.Debug("新的客户端接入：", slog.String("id", sock.Id))
			go agentSocketHandler(sock)
		}, func(sock *network.Socket) {
			slog.Debug("客户端断开连接：", slog.String("id", sock.Id))
		}); err != nil {
			slog.Debug("未与管理平台连接成功，5秒后重试！", slog.Any("err", err))
			time.Sleep(5 * time.Second)
		}
		if !testMgr.IsClosed { //如果服务已经关闭，则不再继续连接管理平台
			slog.Debug("与管理平台断开连接，5秒后重试！")
			time.Sleep(5 * time.Second)
		}
	}
	slog.Debug("与管理平台结束连接！")
	testMgr.Close()
}

func ConnectClientAgent(request *agent.Requst, config *agent.Config) *client.Client {
	proxy, err := agent.GetProxy(request)
	if err != nil {
		return nil
	}
	proxy.ProxyAddr = proxy.ProxyIp + ":" + proxy.ProxyPort
	cli := client.NewClient(proxy.ProxyAddr, request.CliId) //尝试连接本机服务
	agt, err := agent.NewAgent(cli.ServerAddress, uint32(proxy.Idx), 0, config)
	if err == nil && agt != nil {
		_ = cli.ConnectToNet(3, agt.Socket, agt.RemoteAddress, func(sock *network.Socket) {
			slog.Debug("socket已经断开===》！", slog.String("id", sock.Id))
			if agt.Socket != nil {
				slog.Debug("正在与代理断开连接...")
				_ = agt.Socket.Close()
			}
		}) //创建udp网络
	}
	if cli.Socket == nil {
		slog.Info("客户端连接失败！", slog.Int("idx", proxy.Idx))
		return nil
	}

	for i := 0; i < cli.Socket.ChannelCount; i++ {
		go receiveHandler(cli, i)
	}
	msg0 := "hello,i am channel 0 data from client"
	slog.Info("正在向通道0发送数据", slog.String("msg", msg0))
	_, _ = cli.Socket.Send(0, []byte(msg0))
	msg1 := "hello,i am channel 1 data from client"
	slog.Info("正在向通道1发送数据", slog.String("msg", msg1))
	_, _ = cli.Socket.Send(1, []byte(msg1))

	msg2 := "hello,i am channel 2 data from client"
	slog.Info("正在向通道2发送数据", slog.String("msg", msg2))
	_, _ = cli.Socket.Send(2, []byte(msg2))

	return cli
}

func TestClientAgent(t *testing.T) {
	utils.InitLog(slog.LevelDebug, nil)
	//{Ts:1779441784
	//Sign:
	//MgrAddr:https://192.168.199.159:3005
	//Token:0DBDB1AE-CABD-F2BA-4F89-132A39EC90D1
	//DevId:0A76DE8C-1AB1-35C3-A137-FC9E10B1EF9F
	//ProxyId:A7C569D2-F0A7-7B0C-BB2A-E44D59D5A5EB
	//SvrAddr:192.168.199.22
	//ProxyInfo:true
	//CliId:1a1f2dadcd90473bb684bcd02b9cc629
	//NetType:udp
	//SvcType:udp
	//SvcAddr:192.168.199.22
	//LoAddr: UpBuf:0
	//DownBuf:0}
	request := &agent.Requst{
		MgrAddr: "https://192.168.199.159:3005",
		Token:   "0DBDB1AE-CABD-F2BA-4F89-132A39EC90D1",
		DevId:   "0A76DE8C-1AB1-35C3-A137-FC9E10B1EF9F",
		ProxyId: "A7C569D2-F0A7-7B0C-BB2A-E44D59D5A5EB",
		Proxy:   true,
		CliId:   "1a1f2dadcd90473bb684bcd02b9cc629",
		NetType: network.STREAM_NETWORK_UDP,
	}
	cfg := &agent.Config{
		Version:  "1",
		SignSalt: "2fbbdf99eae1675484a48e8310db1ee42d3bd6fdbc5e3f3755af848b23cc9817",
	}
	cli := ConnectClientAgent(request, cfg)
	for {
		if cli == nil || cli.IsClosed {
			break
		}
		_, _ = cli.Socket.Ping(0)
		time.Sleep(5 * time.Millisecond)
	}
}

func TestMultiClientAgent(t *testing.T) {
	utils.InitLog(slog.LevelDebug, nil)
	request := &agent.Requst{
		MgrAddr: "https://192.168.199.159:3005",
		Token:   "0DBDB1AE-CABD-F2BA-4F89-132A39EC90D1",
		DevId:   "0A76DE8C-1AB1-35C3-A137-FC9E10B1EF9F",
		ProxyId: "A7C569D2-F0A7-7B0C-BB2A-E44D59D5A5EB",
		Proxy:   true,
		CliId:   "1a1f2dadcd90473bb684bcd02b9cc629",
		NetType: network.STREAM_NETWORK_UDP,
	}
	cfg := &agent.Config{
		Version:  "1",
		SignSalt: "2fbbdf99eae1675484a48e8310db1ee42d3bd6fdbc5e3f3755af848b23cc9817",
	}
	for i := 0; i < 3; i++ {
		request.CliId += strconv.Itoa(i)
		ConnectClientAgent(request, cfg)
	}
	time.Sleep(time.Second * 10)
}
