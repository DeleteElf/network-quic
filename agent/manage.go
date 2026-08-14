package agent

import "C"
import (
	"crypto/tls"
	"encoding/json"
	"github.com/DeleteElf/zero-net/framework"
	"github.com/DeleteElf/zero-net/framework/network"
	"github.com/DeleteElf/zero-net/framework/utils"
	"github.com/DeleteElf/zero-net/server"
	"github.com/quic-go/quic-go"
	"log/slog"
	"net"
	"time"

	"github.com/gorilla/websocket"
)

type AgentMessageCallbackFunc func(int, string)

type Config struct {
	Id       int64
	MgrAddr  string           `json:"mgr_addr"`
	Hearts   int              `json:"hearts"`
	Data     utils.JsonObject `json:"data"`
	Version  string           `json:"version" default:"1"`
	SignSalt string           `json:"sign_salt" required:"true"`
	//Port    int              `json:"port"` //因为代理中心的问题，暂时无法直接设置端口
}

type AgentStream struct {
	Info   *network.StreamInfo
	Stream *quic.Stream
	Server *server.Server
}

type AcceptClientConnect func(chan AgentStream)

type ManagePlatform struct {
	//用于串联代理中心和主机之间的通讯
	AgentStreamChannel chan AgentStream
	//与管理平台之间的连接
	wsConn *websocket.Conn
	framework.CloseableObject
	Agents map[int]*Agent
	config *Config

	heartTicker *time.Ticker
	//Server          *server.Server //目前代理中心，只能在未建立quic通讯前打通路由，因此不能在此建立服务对象
	lastMessageTime time.Time
}

func NewManagePlatform(cfg *Config) *ManagePlatform {
	mgr := &ManagePlatform{
		AgentStreamChannel: make(chan AgentStream), //创建
		Agents:             make(map[int]*Agent),   //创建代理空间
		config:             cfg,
		lastMessageTime:    time.Now(),
	}
	mgr.config.Id = time.Now().UnixNano()
	mgr.IsClosed = false
	mgr.SetOnCloseHandler(mgr)
	return mgr
}

func (mgr *ManagePlatform) ConnectToPlatform() {
	websocket.DefaultDialer.HandshakeTimeout = 10 * time.Second
	websocket.DefaultDialer.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: true,
	}
	ws, _, err := websocket.DefaultDialer.Dial(mgr.config.MgrAddr, nil)
	if err != nil {
		return
	}
	mgr.wsConn = ws
	data := PlatformActionInfo{
		Action: ACTION_REG,
		From:   "host",
		Info:   mgr.config.Data,
	}
	_ = mgr.sendJson(data)
}

func (mgr *ManagePlatform) OnClosing() bool {
	slog.Debug("正在断开管理平台的连接...")
	if mgr.heartTicker != nil {
		mgr.heartTicker.Stop()
		mgr.heartTicker = nil
	}
	return true
}

func (mgr *ManagePlatform) OnClosed() {
	if mgr.wsConn != nil {
		_ = mgr.wsConn.Close()
		mgr.wsConn = nil
	}
	slog.Debug("管理平台已经断开！")
}

func (mgr *ManagePlatform) sendJson(v any) error {
	jsonString, err := utils.ToJsonString(v)
	if err != nil {
		return err
	}
	return mgr.send(jsonString)
}

func (mgr *ManagePlatform) send(msg string) error {
	if mgr.wsConn != nil {
		slog.Debug("平台发送", slog.String("body", msg))
		return mgr.wsConn.WriteMessage(websocket.TextMessage, []byte(msg))
	}
	return nil
}

func (mgr *ManagePlatform) Hearts() {
	tickerDuration := time.Duration(mgr.config.Hearts) * time.Second
	expireDuration := time.Duration(mgr.config.Hearts+10) * time.Second
	mgr.heartTicker = time.NewTicker(tickerDuration)
	defer func() {
		if mgr.heartTicker != nil {
			mgr.heartTicker.Stop()
			mgr.heartTicker = nil
		}
	}()
	id := mgr.config.Id
	for range mgr.heartTicker.C {
		if mgr.wsConn == nil {
			break
		}
		if mgr.IsClosed {
			break
		}
		if id != mgr.config.Id { //如果已经不是之前的连接，则跳出
			break
		}
		if mgr.lastMessageTime.Add(expireDuration).Compare(time.Now()) < 0 {
			slog.Warn("platform pong time out")
			_ = mgr.wsConn.Close()
			break
		}
		_ = mgr.sendJson(PlatformActionInfo{
			Action: "ping",
			From:   "host",
		})
	}
	slog.Debug("platform stop ping!")
}

func (mgr *ManagePlatform) ListenAgentConnect(onAcceptSocket, onDisconnect network.SocketCallbackFunc) error {
	for {
		if mgr.wsConn == nil {
			break
		}
		if mgr.IsClosed {
			break
		}
		_, msg, err := mgr.wsConn.ReadMessage()
		if err != nil {
			return err
		}
		slog.Debug("平台收到", slog.Any("body", msg))
		var proxy ActionProxy
		if err := json.Unmarshal(msg, &proxy); err != nil {
			slog.Warn("json Unmarshal err", slog.Any("err", err))
			continue
		}
		mgr.lastMessageTime = time.Now()
		if !proxy.IsSuccess() {
			slog.Warn("action not success")
			continue
		}
		if proxy.Action != "proxy" {
			//slog.Debug("mgr resp", slog.String("action", proxy.Action))
			continue
		}
		proxyInfo := proxy.Data
		if len(proxyInfo.ProxyAddr) == 0 {
			proxyInfo.ProxyAddr = proxyInfo.ProxyIp + ":" + proxyInfo.ProxyPort
		}
		count := 1
		if proxyInfo.AllowExternal { //如果允许外网连接
			count = 2
		}
		for i := 0; i < count; i++ {
			if len(proxyInfo.ProxyAddr) == 0 { //平台没有返回有效地址
				continue
			}
			if i == 1 {
				proxyInfo.ProxyAddr = proxyInfo.ProxyExternalIp + ":" + proxyInfo.ProxyExternalPort
			}
			if mgr.Agents[proxyInfo.Idx] == nil { //如果这个代理还没有连接，则进行连接
				slog.Debug("正在连接代理授权地址...", slog.String("addr", proxyInfo.ProxyAddr), slog.Int("idx", proxyInfo.Idx))
				conn, err := network.NewUdpSocketClient()
				if err != nil {
					slog.Error("创建UDP客户端失败", slog.Any("err", err))
					continue
				}
				proxyAddr, err := net.ResolveUDPAddr(network.STREAM_NETWORK_UDP, proxyInfo.ProxyAddr)
				if err != nil {
					slog.Error("解析代理中心地址失败", slog.Any("err", err))
					continue
				}
				agent, err := NewAgentService(conn, proxyAddr, uint32(proxyInfo.Idx), 1, mgr.config)
				if err != nil {
					slog.Error("连接代理失败", slog.Any("err", err))
					continue
				}
				agent.Proxy = &proxyInfo
				agent.Server = server.NewServer(agent.Socket, true)
				agent.Server.OnAcceptSocket = onAcceptSocket
				go agent.Server.StartListen(onDisconnect)
				mgr.Agents[proxyInfo.Idx] = agent //连接成功即可
				slog.Debug("代理服务创建成功！", slog.Int("idx", proxyInfo.Idx))
			}
			break
		}
	}
	return nil
}

func (mgr *ManagePlatform) GetServerSocket(clientId string) *network.Socket {
	if len(clientId) == 0 {
		return nil
	}
	for _, agent := range mgr.Agents {
		if agent.Server.Sockets[clientId] != nil {
			return agent.Server.Sockets[clientId]
		}
	}
	return nil
}

func (mgr *ManagePlatform) GetServer(clientId string) *server.Server {
	if len(clientId) == 0 {
		return nil
	}
	for _, agent := range mgr.Agents {
		if agent.Server.Sockets[clientId] != nil {
			return agent.Server
		}
	}
	return nil
}
