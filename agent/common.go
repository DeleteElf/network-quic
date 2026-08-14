package agent

import (
	"encoding/binary"
	"fmt"
	"github.com/DeleteElf/zero-net/framework/network"
	"github.com/DeleteElf/zero-net/framework/utils"
	"github.com/DeleteElf/zero-net/server"
	"log/slog"
	"net"
	"strconv"
	"time"
)

const (
	ACTION_REG      = "register"
	AGENT_PKG_SIZE  = 4096
	AGENT_HEAD_SIZE = 4
	AGENT_PEER_SIZE = 16

	PROXY_MAGIC         = "CGTW"
	PROXY_FLAG_DEST_SVR = 0x01
	PROXY_FLAG_AUTH_REQ = 0x02
	PROXY_FLAG_AUTH_SYN = 0x04
)

type ProxyInfo struct {
	ProxyIp           string `json:"proxy_ip"`
	ProxyPort         string `json:"proxy_port"`
	ProxyExternalIp   string `json:"proxy_external_ip"`   //管理平台的外网ip
	ProxyExternalPort string `json:"proxy_external_port"` //管理平台的外网端口
	ProxyAddr         string //实际使用的地址
	Idx               int    `json:"idx"`
	AllowExternal     bool   `json:"allow_external" default:"false"` //是否允许外网连接
}

type ActionProxy struct {
	Success interface{} `json:"success"`
	Type    string      `json:"type"` // 一般是json
	Action  string      `json:"action"`
	Data    ProxyInfo   `json:"data"`
}

// PlatformActionInfo 连接管理平台的信息
type PlatformActionInfo struct {
	Action string           `json:"action"`
	From   string           `json:"from"`
	Info   utils.JsonObject `json:"info"`
}

func (a *ActionProxy) IsSuccess() bool {
	if b, ok := a.Success.(bool); ok {
		return b
	}

	if i, ok := a.Success.(int); ok {
		return i != 0
	}

	if s, ok := a.Success.(string); ok {
		if i, err := strconv.Atoi(s); err == nil {
			return i != 0
		} else {
			return false
		}
	}
	return false
}

type Agent struct {
	//基础网络连接
	NetConn net.PacketConn
	//接入的会话列表
	Socket *Socket
	//当前代理的远程地址
	RemoteAddress net.Addr
	//当前主机的代理配置信息
	Proxy  *ProxyInfo
	Server *server.Server
	Config *Config
}

// NewAgentService 创建新的代理服务，支持客户端和服务端
func NewAgentService(conn net.PacketConn, proxyAddr net.Addr, idx uint32, flag byte, config *Config) (*Agent, error) {
	agt := &Agent{
		NetConn:       conn,
		RemoteAddress: proxyAddr,
		Config:        config,
	}
	err := agt.addAuthAgent(idx, flag)
	if err != nil {
		return nil, err
	}
	return agt, err
}

// NewAgent 创建新的客户端代理
func NewAgent(addr string, idx uint32, flag byte, config *Config) (*Agent, error) {
	conn, err := network.NewUdpSocketClient()
	if err != nil {
		slog.Error("创建UDP客户端失败", slog.Any("err", err))
		return nil, err
	}
	proxyAddr, err := net.ResolveUDPAddr(network.STREAM_NETWORK_UDP, addr)
	if err != nil {
		slog.Error("解析代理地址失败", slog.Any("err", err))
		return nil, err
	}
	return NewAgentService(conn, proxyAddr, idx, flag, config)
}

func (agt *Agent) addAuthAgent(idx uint32, flag byte) error {
	agentConn := &Socket{
		Conn:      agt.NetConn,
		Idx:       idx,
		ReadData:  make([]byte, AGENT_PKG_SIZE),
		WriteData: make([]byte, AGENT_PKG_SIZE),
	}
	binary.LittleEndian.PutUint32(agentConn.WriteData, idx)
	var head [AGENT_PEER_SIZE]byte
	binary.LittleEndian.PutUint32(head[:], idx)
	head[3] = (flag & PROXY_FLAG_DEST_SVR) | PROXY_FLAG_AUTH_SYN
	copy(head[4:8], []byte(PROXY_MAGIC))
	err := agt.authAgent(head[:])
	if err != nil {
		return err
	}
	agt.Socket = agentConn
	slog.Debug("加入代理socket", slog.Any("idx", idx))
	return nil
}

type Auth struct {
	Ts   int64  `json:"t"`
	Ver  string `json:"v"` // 客户端的版本号
	Sign string `json:"s"` // 客户端的签名，用于服务端校验客户端的合法性
}

func (agt *Agent) authAgent(head []byte) error {
	conn := agt.NetConn
	proxyAddr := agt.RemoteAddress
	buffer := make([]byte, 4096)
	var strAddr string
	for attempt := 1; attempt <= 3; attempt++ {
		slog.Debug("send syn", slog.Any("proxyAddr", proxyAddr))
		_, err := conn.WriteTo(head, proxyAddr)
		if err != nil {
			return err
		}
		err = conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		if err != nil {
			slog.Info("SetReadDeadline", slog.Any("proxyAddr", proxyAddr), slog.Any("err", err))
			return err
		}
		n, _, err := conn.ReadFrom(buffer)
		if err != nil {
			slog.Info("ReadFrom", slog.Any("proxyAddr", proxyAddr), slog.Any("err", err))
			continue
		}

		strAddr = fmt.Sprintf("%d.%d.%d.%d:%d", buffer[4], buffer[5], buffer[6], buffer[7], binary.BigEndian.Uint16(buffer[8:]))

		slog.Debug("recv syn", slog.Any("proxyAddr", proxyAddr), slog.Any("data", buffer[:n]), slog.String("addr", strAddr))
		break
	}

	if len(strAddr) == 0 {
		return fmt.Errorf("timeout")
	}

	info := Auth{
		Ts:  time.Now().Unix(),
		Ver: agt.Config.Version,
	}
	info.Sign = utils.EncryptBytes([]byte(fmt.Sprintf("%s_%s_%d", strAddr, agt.Config.SignSalt, info.Ts)))
	jsonInfo, err := utils.ToJsonByte(info)
	if err != nil {
		return err
	}
	data := make([]byte, AGENT_PEER_SIZE+len(jsonInfo))
	copy(data[:AGENT_PEER_SIZE], head)
	copy(data[AGENT_PEER_SIZE:], []byte(jsonInfo))
	data[3] = (head[3] & PROXY_FLAG_DEST_SVR) | PROXY_FLAG_AUTH_REQ

	for attempt := 1; attempt <= 3; attempt++ {
		slog.Debug("send req", slog.Any("proxyAddr", proxyAddr))
		_, err := conn.WriteTo(data, proxyAddr)
		if err != nil {
			return err
		}

		err = conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		if err != nil {
			slog.Info("SetReadDeadline", slog.Any("proxyAddr", proxyAddr), slog.Any("err", err))
			return err
		}
		n, _, err := conn.ReadFrom(buffer)
		if err != nil {
			slog.Debug("ReadFrom", slog.Any("proxyAddr", proxyAddr), slog.Any("err", err))
			continue
		}
		if n < AGENT_PEER_SIZE || buffer[3] != data[3] {
			continue
		}

		err = conn.SetReadDeadline(time.Time{})
		if err != nil {
			slog.Info("SetReadDeadline none", slog.Any("proxyAddr", proxyAddr), slog.Any("err", err))
			continue
		}
		slog.Debug("recv req", slog.Any("proxyAddr", proxyAddr), slog.Any("data", buffer[:n]))
		return nil
	}
	return fmt.Errorf("timeout")
}
