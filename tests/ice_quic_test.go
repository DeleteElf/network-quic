package tests

//
//import (
//	"bytes"
//	"crypto/tls"
//	"errors"
//	"github.com/DeleteElf/zero-net/client"
//	"github.com/DeleteElf/zero-net/framework/streams"
//	"github.com/DeleteElf/zero-net/framework/utils"
//	"github.com/DeleteElf/zero-net/server"
//	"github.com/DeleteElf/zero-net/stunhelper"
//	"github.com/DeleteElf/zero-net/websocket"
//	"github.com/deleteelf/goframework/utils/jsonhelper"
//	"io"
//	"log/slog"
//	"net"
//	"net/http"
//	"strconv"
//	"strings"
//	"testing"
//	"time"
//)
//
//func DetectStun(stun, token string, conn net.PacketConn) (string, int) {
//	if len(stun) > 0 {
//		slog.Debug("配置了stun服务，正在准备探测！", slog.String("address", stun))
//		cli := stunhelper.NewClient()
//		err := cli.Connect(stun, token, conn)
//		if err == nil {
//			slog.Debug("你的公网 IP 地址 :", slog.Any("ip", cli.ExternalAddress.IP))
//			slog.Debug("你的公网映射端口 : ", slog.Int("port", cli.ExternalAddress.Port))
//			return cli.ExternalAddress.IP.String(), cli.ExternalAddress.Port
//		}
//		//client.Close()
//	}
//	return "", 0
//}
//
//func PunchHole(conn net.PacketConn, targetAddr net.Addr, timeout time.Duration) error {
//	slog.Info("客户端：开始异步向服务端盲发 UDP 包冲刷 NAT 洞口...", slog.String("target", targetAddr.String()))
//	go func() {
//		pingMsg := []byte("{\"action\":\"ping\",\"from\":\"iceClient\"}")
//		// 密集发送 20 个 UDP 包（持续 600ms），把客户端 NAT 防火墙对服务端的出口映射彻底打开
//		for i := 0; i < 20; i++ {
//			_, _ = conn.WriteTo(pingMsg, targetAddr)
//			time.Sleep(30 * time.Millisecond)
//		}
//		slog.Debug("客户端：NAT 出口冲刷完成！")
//	}()
//	return nil
//}
//
//func ConnectByStun(cli *client.Client, token, stunKey string, channelCount int, onDisconnect streams.SocketCallbackFunc) error {
//	var err error
//	cli.NetConn, err = streams.NewUdpSocketClient()
//	if err != nil {
//		slog.Error("创建UDP客户端失败", slog.Any("err", err))
//		cli.Close()
//		return err
//	}
//	serverAddress := cli.ServerAddress
//	if len(cli.Stun) > 0 { //如果配置了stun服务器
//		localIp := stunhelper.GetLocalAddress(cli.Stun)
//		remoteAddress, port := DetectStun(cli.Stun, stunKey, cli.NetConn)
//
//		slog.Info("客户端 STUN 解析结果", slog.String("remoteAddress", remoteAddress), slog.Int("port", port))
//		if remoteAddress == "" || port == 0 {
//			slog.Error("客户端 STUN 探测失败，无法获取公网地址！")
//			return errors.New("STUN detect failed")
//		}
//
//		strs := strings.Split(cli.NetConn.LocalAddr().String(), ":")
//		data := jsonhelper.JsonObject{}
//		data["type"] = "offer"
//		data["sdp"] = "candidate:1 1 UDP 2130706431 " + remoteAddress + " " + strconv.Itoa(port) + " typ srflx raddr " + localIp + " rport " + strs[len(strs)-1]
//		jsonData, err := jsonhelper.ToJsonString(data)
//		if err != nil {
//			slog.Error("转成json过程出错！", slog.Any("err", err))
//			return err
//		}
//
//		tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
//		httpClient := &http.Client{Transport: tr}
//		request, err := http.NewRequest(http.MethodPost, "https://192.168.199.159:3005/ice?device_id=0A76DE8C-1AB1-35C3-A137-FC9E10B1EF9F",
//			bytes.NewBufferString(jsonData))
//		if err != nil {
//			slog.Error("生成http出错！", slog.Any("err", err))
//			return err
//		}
//		request.Header.Set("Authorization", token)
//		resp, err := httpClient.Do(request)
//		slog.Debug("发送数据", slog.String("data", jsonData))
//		if err != nil {
//			slog.Error("请求http出错！", slog.Any("err", err))
//			return err
//		}
//		defer resp.Body.Close()
//		if resp.StatusCode != http.StatusOK {
//			slog.Error("请求http失败！", slog.String("错误码", resp.Status))
//			return nil
//		}
//		body, err := io.ReadAll(resp.Body)
//		if err != nil {
//			slog.Error("读取http应答的body出错！", slog.Any("err", err))
//		}
//		slog.Info("收到http应答：", slog.String("body", string(body)))
//		result, err := jsonhelper.GetJsonObject(body)
//
//		if result["data"] != nil {
//			sdpData := result["data"].(map[string]interface{})
//			sdpDatas := strings.Split(sdpData["sdp"].(string), " ")
//			serverAddress = net.JoinHostPort(sdpDatas[4], sdpDatas[5])
//		}
//	}
//	netAddr, err := net.ResolveUDPAddr(network.STREAM_NETWORK_UDP, serverAddress)
//	if err != nil {
//		slog.Error("解析服务端地址失败", slog.Any("err", err))
//		return err
//	}
//	if len(cli.Stun) > 0 {
//		err = PunchHole(cli.NetConn, netAddr, 5*time.Second)
//		if err != nil {
//			slog.Error("UDP 打洞失败，放弃 QUIC 连接", slog.Any("err", err))
//			return err
//		}
//		// 2. 稍微等待 200ms，让双方的打洞 UDP 包在公网路上“相遇”并建立 NAT 映射轨
//		time.Sleep(200 * time.Millisecond)
//	}
//	return cli.ConnectToNet(channelCount, cli.NetConn, netAddr, onDisconnect)
//}
//
//func TestIceClient(t *testing.T) {
//	utils.InitLog(slog.LevelDebug, nil)   //初始化日志
//	cli := client.NewClient("", "test01") //尝试连接外网本机服务
//	cli.Stun = "stun:stun.new0.com.cn:3478"
//
//	err := ConnectByStun(cli, "0DBDB1AE-CABD-F2BA-4F89-132A39EC90D1", "test", 3, func(sock *streams.Socket) {
//		slog.Debug("socket已经断开===》！", slog.String("clientId", sock.Id))
//	}) //创建udp网络
//
//	if err != nil {
//		return
//	}
//	slog.Info("客户端连接成功！", slog.Int("通道数", cli.Socket.ChannelCount))
//	for i := 0; i < cli.Socket.ChannelCount; i++ {
//		go receiveHandler(cli, i)
//	}
//	msg0 := "hello,i am channel 0 data from client"
//	slog.Info("正在向通道0发送数据", slog.String("msg", msg0))
//	_, _ = cli.Socket.Send(0, []byte(msg0))
//	msg1 := "hello,i am channel 1 data from client"
//	slog.Info("正在向通道1发送数据", slog.String("msg", msg1))
//	_, _ = cli.Socket.Send(1, []byte(msg1))
//
//	msg2 := "hello,i am channel 2 data from client"
//	slog.Info("正在向通道2发送数据", slog.String("msg", msg2))
//	_, _ = cli.Socket.Send(2, []byte(msg2))
//
//	//time.Sleep(time.Second * 3) //等待3秒，等他们通讯完成再退出
//	for {
//		if cli.IsClosed || cli.Socket.IsClosed {
//			break
//		} else {
//			_, _ = cli.Socket.Ping(0)
//			time.Sleep(time.Millisecond * 10)
//		}
//	}
//}
//
//func TestIceServer(t *testing.T) {
//	utils.InitLog(slog.LevelDebug, nil)
//	ws := websocket.NewClient()
//	ws.HeartTimeout = time.Second * 5
//	ws.OnConnected = func(address string) {
//		slog.Info("与服务端连接", slog.String("address", address))
//		ws.Send("{\"action\":\"register\",\"appid\":0,\"info\":{\"hostname\":\"PC-PAQHBVPFDXTY\",\"version\":\"1.0.26.080501\",\"appVersion\":\"7.1.431.-1\",\"gfeVersion\":\"3.23.0.74\",\"cpuid\":\"0000001068747541444D416369746E65_34-5A-60-7B-98-E1\",\"cpu\":\"AMD Ryzen 5 5600GT with Radeon Graphics\",\"mac\":\"34-5A-60-7B-98-E1\",\"ip\":\"192.168.199.22\",\"gpu\":\"\",\"ram\":\"27.90 GB (3.89 GB 可用)\",\"os\":\"windows NT 10 22H2\",\"apps\":[{\"id\":\"10000\",\"name\":\"Desktop\"}]}}")
//	}
//	ws.OnDisconnected = func(reason string) {
//		slog.Info("与服务端断开连接", slog.String("reason", reason))
//		if ws.Reconnect && !ws.IsClosed {
//			_ = ws.Connect(ws.Address, ws.HeartMessage)
//		}
//	}
//
//	// 1. 初始化 Server 实例并确保绑定端口/创建 NetConn
//	testServer = server.NewServerByAddress("0.0.0.0:10001")
//	testServer.Stun = "stun:stun.new0.com.cn:3478"
//
//	// ⚠️ 【关键修正】：确保 testServer.NetConn 不为 nil 后再调用 DetectStun
//	if testServer.NetConn == nil {
//		addr, _ := net.ResolveUDPAddr(network.STREAM_NETWORK_UDP, "0.0.0.0:10001")
//		testServer.NetConn, _ = net.ListenUDP(network.STREAM_NETWORK_UDP, addr)
//	}
//
//	remoteAddress, port := DetectStun(testServer.Stun, "test", testServer.NetConn)
//	slog.Info("服务端 STUN 解析结果", slog.String("remoteAddress", remoteAddress), slog.Int("port", port))
//
//	iceMessage := make(chan string)
//
//	ws.OnMessage = func(msg string) {
//		data, err := jsonhelper.GetJsonObject([]byte(msg))
//		if err == nil && data["data"] != nil {
//			body := data["data"].(map[string]interface{})
//			if body["type"] != nil && body["sdp"] != nil && body["type"].(string) == "offer" {
//				result := jsonhelper.JsonObject{}
//				sdpBody := jsonhelper.JsonObject{}
//				result["success"] = "true"
//				result["action"] = data["action"].(string)
//				result["type"] = "response"
//				if body["session_id"] != nil {
//					result["session_id"] = body["session_id"].(string)
//				}
//
//				localAddress, err := net.ResolveUDPAddr(network.STREAM_NETWORK_UDP, ws.Conn.LocalAddr().String())
//				if err == nil {
//					sdpBody["type"] = "answer"
//					sdpBody["sdp"] = "candidate:1 1 UDP 2130706431 " + remoteAddress + " " + strconv.Itoa(port) + " typ srflx raddr " + localAddress.IP.String() + " rport 10001"
//					result["data"] = sdpBody
//					re, e := jsonhelper.ToJsonString(result)
//					if e == nil {
//						_ = ws.Send(re)
//					}
//
//					if body["sdp"] != nil {
//						datas := strings.Split(body["sdp"].(string), " ")
//						if len(datas) >= 6 {
//							addr := net.JoinHostPort(datas[4], datas[5])
//							iceMessage <- addr
//						}
//					}
//				}
//			}
//		}
//	}
//	go func() {
//		err := ws.Connect("wss://192.168.199.159:3005/device?type=device&apikey=575D6618206A2754", websocket.DefaultHeartMessage)
//		if err != nil {
//			slog.Error("连接发生错误", slog.Any("err", err))
//		}
//	}()
//	testServer.OnAcceptSocket = func(sock *streams.Socket) {
//		slog.Debug("新的客户端接入：", slog.String("id", sock.Id))
//		go socketHandler(sock)
//	}
//	// 打洞监听协程
//	go func() {
//		for {
//			select {
//			case addr := <-iceMessage:
//				slog.Debug("收到请求探测新的地址", slog.String("addr", addr))
//				if len(addr) > 10 {
//					clientAddr, err := net.ResolveUDPAddr(network.STREAM_NETWORK_UDP, addr)
//					if err == nil {
//						go func() {
//							for i := 0; i < 30; i++ {
//								testServer.NetConn.WriteTo([]byte("{\"action\":\"ping\",\"from\":\"iceServer\"}"), clientAddr)
//								time.Sleep(30 * time.Millisecond)
//							}
//							slog.Debug("服务端：NAT 出口冲刷完成！")
//						}()
//
//					}
//				}
//				break
//			}
//		}
//	}()
//
//	for {
//		if restart {
//			time.Sleep(1 * time.Second)
//			slog.Debug("服务端重新启动监听！")
//			restart = false
//		}
//		testServer.StartListen(func(sock *streams.Socket) {
//			slog.Debug("客户端断开连接：", slog.String("id", sock.Id))
//		})
//		slog.Debug("服务端退出监听！")
//		if !restart {
//			break
//		}
//	}
//}
