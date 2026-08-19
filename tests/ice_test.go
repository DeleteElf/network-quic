package tests

import (
	"bytes"
	"github.com/DeleteElf/zero-net/client"
	"github.com/DeleteElf/zero-net/framework/network"
	"github.com/DeleteElf/zero-net/framework/utils"
	"github.com/DeleteElf/zero-net/server"
	"github.com/DeleteElf/zero-net/websocket"
	"log/slog"
	"net/http"
	"testing"
	"time"
)

func TestIceClient(t *testing.T) {
	utils.InitLog(slog.LevelDebug, nil)   //初始化日志
	cli := client.NewClient("", "test01") //尝试连接外网本机服务
	cli.Stun = []string{"stun:stun.l.google.com:19302"}
	if len(cli.Stun) > 0 {
		offer, err := cli.DetectStunByDefault()
		data := utils.JsonObject{}
		data["type"] = "offer"
		data["sdp"] = offer //"a=candidate:1 1 UDP 2130706431 " + remoteAddress + " " + strconv.Itoa(port) + " typ srflx raddr " + localIp + " rport " + strs[len(strs)-1]
		jsonData, err := utils.ToJsonString(data)
		body, err := network.HttpRequest("https://192.168.199.159:3005/ice?device_id=0A76DE8C-1AB1-35C3-A137-FC9E10B1EF9F",
			http.MethodPost, "0DBDB1AE-CABD-F2BA-4F89-132A39EC90D1", bytes.NewBufferString(jsonData))
		if err != nil {
			slog.Error("读取http应答的body出错！", slog.Any("err", err))
		}
		slog.Info("收到http应答：", slog.String("body", string(body)))
		result, err := utils.GetJsonObject(body)
		if result["data"] != nil {
			answer := result["data"].(map[string]interface{})
			if answer["sdp"] != nil {
				conn := cli.PunchHole(answer["sdp"].(string), 30*time.Second, false)
				cli.ConnectByIce(conn)
			}
		}
	} else {
		//todo:这里使用普通连接
	}

	for {
		if cli.IsClosed {
			break
		} else {
			time.Sleep(time.Second)
		}
	}
}

func TestIceServer(t *testing.T) {
	utils.InitLog(slog.LevelDebug, nil)
	ws := websocket.NewClient()
	ws.HeartTimeout = time.Second * 5
	ws.OnConnected = func(address string) {
		slog.Info("与服务端连接", slog.String("address", address))
		ws.Send("{\"action\":\"register\",\"appid\":0,\"info\":{\"hostname\":\"PC-PAQHBVPFDXTY\",\"version\":\"1.0.26.080501\",\"appVersion\":\"7.1.431.-1\",\"gfeVersion\":\"3.23.0.74\",\"cpuid\":\"0000001068747541444D416369746E65_34-5A-60-7B-98-E1\",\"cpu\":\"AMD Ryzen 5 5600GT with Radeon Graphics\",\"mac\":\"34-5A-60-7B-98-E1\",\"ip\":\"192.168.199.22\",\"gpu\":\"\",\"ram\":\"27.90 GB (3.89 GB 可用)\",\"os\":\"windows NT 10 22H2\",\"apps\":[{\"id\":\"10000\",\"name\":\"Desktop\"}]}}")
	}
	ws.OnDisconnected = func(reason string) {
		slog.Info("与服务端断开连接", slog.String("reason", reason))
		if ws.Reconnect && !ws.IsClosed {
			_ = ws.Connect(ws.Address, ws.HeartMessage)
		}
	}

	// 1. 初始化 Server 实例并确保绑定端口/创建 NetConn
	testServer = server.NewServer(nil, false) //server.NewServerByAddress("0.0.0.0:10001")
	//testServer.Stun = []string{"stun:stun.l.google.com:19302", "stun:stun.new0.com.cn:3478"}
	testServer.Stun = []string{"stun:stun.l.google.com:19302"}
	var localInfo utils.JsonObject
	if len(testServer.Stun) > 0 {
		answer, _ := testServer.DetectStun(10005, 10006) //阿里云极限测试
		localInfo = utils.JsonObject{}
		localInfo["type"] = "answer"
		localInfo["sdp"] = answer //"a=candidate:1 1 UDP 2130706431 " + remoteAddress + " " + strconv.Itoa(port) + " typ srflx raddr " + localIp + " rport " + strs[len(strs)-1]
	}

	ws.OnMessage = func(msg string) {
		data, err := utils.GetJsonObject([]byte(msg))
		if err == nil && data["data"] != nil {
			body := data["data"].(map[string]interface{})
			action := data["action"].(string)
			switch action {
			//case "ice_state":
			//	{
			//		if body["session_id"] != nil {
			//			sessionId := body["session_id"].(string)
			//			select {
			//			case <-testServer.IceChannel:
			//				result := utils.JsonObject{}
			//				result["success"] = "true"
			//				result["action"] = "ice_state"
			//				result["type"] = "response"
			//				result["session_id"] = sessionId
			//				msg, _ := utils.ToJsonString(result)
			//				slog.Debug("发送消息", slog.String("msg", msg))
			//				_ = ws.Send(msg)
			//				//go func() {
			//				//	testServer.StartListen(func(sock *network.Socket) {
			//				//		slog.Debug("客户端断开连接：", slog.String("id", sock.Id))
			//				//	})
			//				//}()
			//				break
			//			}
			//		}
			//		break
			//	}
			case "ice":
				if body["type"] != nil && body["sdp"] != nil && body["type"].(string) == "offer" {
					result := utils.JsonObject{}
					//sdpBody := utils.JsonObject{}
					result["success"] = "true"
					result["action"] = data["action"].(string)
					result["type"] = "response"
					sessionId := ""
					if body["session_id"] != nil {
						sessionId = body["session_id"].(string)
						result["session_id"] = sessionId
					}
					result["data"] = localInfo
					re, e := utils.ToJsonString(result)
					if len(re) > 0 && e == nil {
						slog.Debug("发送本机信令数据给客户端", slog.String("data", re))
						_ = ws.Send(re)
					}
					conn := testServer.PunchHole(body["sdp"].(string), 30*time.Second, true)
					testServer.ConnectByIce(conn)
					go func() {
						testServer.StartListen(func(sock *network.Socket) {
							slog.Debug("客户端断开连接：", slog.String("id", sock.Id))
						})
					}()
				}
				break
			default:
				break
			}
		}
	}
	go func() {
		err := ws.Connect("wss://192.168.199.159:3005/device?type=device&apikey=575D6618206A2754", websocket.DefaultHeartMessage)
		if err != nil {
			slog.Error("连接发生错误", slog.Any("err", err))
		}
	}()
	testServer.OnAcceptSocket = func(sock *network.Socket) {
		slog.Debug("新的客户端接入：", slog.String("id", sock.Id))
		go socketHandler(testServer, sock)
	}

	for {
		if restart {
			time.Sleep(1 * time.Second)
			slog.Debug("服务端重新启动监听！")
			restart = false
		}
		//testServer.StartListen(func(sock *streams.Socket) {
		//	slog.Debug("客户端断开连接：", slog.String("id", sock.Id))
		//})

		// 直接从底层的 net.PacketConn 中读取原生 UDP 数据包
		for {
			if testServer.IsClosed {
				break
			}
			time.Sleep(time.Second * 1)
		}
		if !restart {
			break
		}
	}
}
