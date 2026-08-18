package ice

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"github.com/deleteelf/goframework/utils/jsonhelper"
	"github.com/pion/ice/v4"
	"log/slog"
	"net"
	"strings"
	"time"
)

// icePacketConn 将 ice.Conn 适配为 net.PacketConn 接口
type icePacketConn struct {
	*ice.Conn
}

// NewICEPacketConn 创建 net.PacketConn 包装器
func NewICEPacketConn(c *ice.Conn) net.PacketConn {
	return &icePacketConn{Conn: c}
}

// ReadFrom 读取数据并返回 ICE 连接的 RemoteAddr
func (c *icePacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	n, err := c.Conn.Read(p)
	if err != nil {
		return 0, nil, err
	}
	return n, c.Conn.RemoteAddr(), nil
}

// WriteTo 忽略传入的 addr 参数，直接通过 ICE 连接发送
func (c *icePacketConn) WriteTo(p []byte, _ net.Addr) (int, error) {
	return c.Conn.Write(p)
}

// IceWorkInterface 可关闭对象
type IceWorkInterface interface {
	DetectStun(portMin, portMax uint16) (string, error)
	PunchHole(message string, timeout time.Duration, isServer bool) net.PacketConn
	ConnectByIce(conn net.PacketConn) bool
}

type IceWorker struct {
	Stun  []string
	Agent *ice.Agent
	IceWorkInterface
}

type SignalInfo struct {
	Ufrag      string   `json:"ufrag"`
	Pwd        string   `json:"pwd"`
	Candidates []string `json:"candidates"`
}

// DetectStun 探测stun服务获取公网ip和端口
func (iw *IceWorker) DetectStun(portMin, portMax uint16) (offer string, err error) {
	config := &ice.AgentConfig{
		NetworkTypes: []ice.NetworkType{ice.NetworkTypeUDP4},
		Urls: func() []*ice.URL {
			urls := []*ice.URL{}
			for _, s := range iw.Stun {
				slog.Debug("加入stun地址", slog.String("url", s))
				u, _ := ice.ParseURL(s)
				urls = append(urls, u)
			}
			return urls
		}(),
	}
	if portMin != 0 && portMax != 0 { //等于0时，不用配置
		config.PortMin = portMin
		config.PortMax = portMax
		//// 显式告知 pion/ice：我的公网 IP 就是这个，不需要 STUN 探测
		//config.NAT1To1IPs = []string{"121.41.228.111"}
		//// 针对 1:1 NAT 环境的 Candidate 类型配置
		//config.NAT1To1IPCandidateType = ice.CandidateTypeHost
		if portMax-portMin < 5 {
			slog.Warn("当端口范围太小时，会导致无法探测公网地址，建议范围不小于5！")
		}
	}

	// 1. 创建 pion/ice Agent 配置
	iw.Agent, err = ice.NewAgent(config)
	if err != nil {
		panic(err)
	}

	// 2. 收集候选地址 (Candidates)
	candidateChan := make(chan ice.Candidate, 10)
	err = iw.Agent.OnCandidate(func(c ice.Candidate) {
		if c != nil {
			candidateChan <- c
		}
	})
	if err != nil {
		panic(err)
	}

	slog.Debug("[ICE] 正在收集公网/局域网 Candidate...")
	if err := iw.Agent.GatherCandidates(); err != nil {
		panic(err)
	}

	// 等待一小会儿收集候选
	time.Sleep(2 * time.Second)

	// 获取本地的 uFrag 和 pwd
	uFrag, pwd, err := iw.Agent.GetLocalUserCredentials()
	if err != nil {
		panic(err)
	}
	slog.Debug("本地身份和密码", slog.String("ufrag", uFrag), slog.String("pwd", pwd))
	// 导出本地信息（准备通过信令发送给对端）
	localCandidates, err := iw.Agent.GetLocalCandidates()
	localInfo := SignalInfo{
		Ufrag:      uFrag,
		Pwd:        pwd,
		Candidates: []string{},
	}
	for _, c := range localCandidates {
		candidate := c.Marshal()
		slog.Debug("加入候选", slog.String("地址", candidate))
		localInfo.Candidates = append(localInfo.Candidates, candidate)
	}

	localJSON, _ := jsonhelper.ToJsonByte(localInfo)
	localB64 := base64.StdEncoding.EncodeToString(localJSON)

	return localB64, nil
}

// PunchHole 【客户端打洞】：持续向服务端发包开洞，并等待服务端的回应
func (iw *IceWorker) PunchHole(message string, timeout time.Duration, isServer bool) net.PacketConn {
	if iw.Agent == nil {
		slog.Warn("请先创建探测stun，再执行打洞！")
		return nil
	}
	remoteB64 := strings.TrimSpace(message)
	remoteJSON, _ := base64.StdEncoding.DecodeString(remoteB64)
	var remoteInfo SignalInfo
	_ = json.Unmarshal(remoteJSON, &remoteInfo)

	// 设置对端凭证与候选地址
	err := iw.Agent.SetRemoteCredentials(remoteInfo.Ufrag, remoteInfo.Pwd)
	if err != nil {
		panic(err)
	}

	for _, cStr := range remoteInfo.Candidates {
		c, err := ice.UnmarshalCandidate(cStr)
		if err == nil {
			_ = iw.Agent.AddRemoteCandidate(c)
		}
	}

	// 4. 开始 ICE 打洞连通性检查
	slog.Debug("[ICE] 开始连通性检查 / 打洞中...")
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var iceConn *ice.Conn
	if isServer {
		iceConn, err = iw.Agent.Accept(ctx, remoteInfo.Ufrag, remoteInfo.Pwd)
	} else {
		iceConn, err = iw.Agent.Dial(ctx, remoteInfo.Ufrag, remoteInfo.Pwd)
	}

	if err != nil {
		slog.Debug("[ICE] 打洞失败: %v\n", err)
		return nil
	}

	slog.Debug("[ICE] 🎉 打洞成功！UDP 链路已就绪。")
	return NewICEPacketConn(iceConn)
}
