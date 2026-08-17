package ice

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/deleteelf/goframework/utils/jsonhelper"
	"github.com/pion/ice/v4"
	"github.com/quic-go/quic-go"
	"log/slog"
	"net"
	"strings"
	"time"
)

//type ConnectionState int
//
//const (
//	None ConnectionState = iota
//	Connecting
//	Connected
//	Closing
//)

//type IceObject struct {
//	SessionId string
//	Ip        string
//	Port      int
//	State     ConnectionState
//}

type IceWorker struct {
	Stun    []string
	NetConn net.PacketConn
	//IceChannel chan IceObject
	QuicConn *quic.Conn
	//IsInQuic   bool
	Agent *ice.Agent
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
				u, _ := ice.ParseURL(s)
				urls = append(urls, u)
			}
			return urls
		}(),
	}
	if portMin != 0 { //等于0时，不用配置
		config.PortMin = portMin
	}
	if portMax != 0 { //等于0时，不用配置
		config.PortMax = portMax
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

	fmt.Println("[ICE] 正在收集公网/局域网 Candidate...")
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
func (iw *IceWorker) PunchHole(message string, timeout time.Duration, isServer bool) *ice.Conn {
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
	fmt.Println("[ICE] 开始连通性检查 / 打洞中...")
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var iceConn *ice.Conn
	if isServer {
		iceConn, err = iw.Agent.Accept(ctx, remoteInfo.Ufrag, remoteInfo.Pwd)
	} else {
		iceConn, err = iw.Agent.Dial(ctx, remoteInfo.Ufrag, remoteInfo.Pwd)
	}

	if err != nil {
		fmt.Printf("[ICE] 打洞失败: %v\n", err)
		return nil
	}

	fmt.Println("[ICE] 🎉 打洞成功！UDP 链路已就绪。")
	return iceConn
}
