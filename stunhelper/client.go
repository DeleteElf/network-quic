package stunhelper

import (
	"context"
	"github.com/DeleteElf/zero-net/framework"
	"github.com/DeleteElf/zero-net/framework/network"
	"github.com/pion/stun/v3"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	//stun服务地址
	Stun []string

	ExternalAddress stun.XORMappedAddress
	LocalAddress    stun.XORMappedAddress
	framework.CloseableObject
}

func NewClient() *Client {
	return &Client{}
}

func GetLocalAddress(stunAddress string) (ip string, port int) {
	uri, err := stun.ParseURI(stunAddress)
	if err != nil {
		slog.Error("stun地址配置错误", slog.Any("err", err))
		return ip, port
	}
	addr := net.JoinHostPort(uri.Host, strconv.Itoa(uri.Port))
	serverAddr, err := net.ResolveUDPAddr(network.STREAM_NETWORK_UDP, addr)
	if err != nil {
		slog.Error("stun地址解析失败！", slog.Any("err", err))
		return ip, port
	}
	conn, err := net.DialUDP(network.STREAM_NETWORK_UDP, nil, serverAddr)
	if err != nil {
		slog.Error("连接目标服务器失败", slog.Any("err", err))
		return ip, port
	}
	datas := strings.Split(conn.LocalAddr().String(), ":")
	ip = datas[0]
	port, _ = strconv.Atoi(datas[1])
	return ip, port
}

func (c *Client) Connect(address, token string, conn net.PacketConn) error {
	//address stun:stun.l.google.com:19302
	// 1. 创建指向公共 STUN 服务器的 UDP 连接 (这里以谷歌公共服务器为例)
	uri, err := stun.ParseURI(address)
	if err != nil {
		//log.Fatalf("解析 STUN URI 失败: %v", err)
		return err
	}
	var cli *stun.Client
	if conn == nil {
		cli, err = stun.DialURI(uri, &stun.DialConfig{})
	} else {
		var stunAddr *net.UDPAddr
		addr := net.JoinHostPort(uri.Host, strconv.Itoa(uri.Port))
		stunAddr, err = net.ResolveUDPAddr(network.STREAM_NETWORK_UDP, addr)
		netConn := NewNetPacketConnection(conn, stunAddr)
		cli, err = stun.NewClient(netConn, stun.WithRTO(2*time.Second))
		slog.Debug("本地局域网地址：", slog.String("ip", conn.LocalAddr().String()))
	}
	if err != nil {
		//log.Fatalf("连接 STUN 服务器失败: %v", err)
		return err
	}

	// 2. 构建一个绑定请求 (Binding Request)
	message := stun.MustBuild(stun.TransactionID, stun.BindingRequest)
	if len(token) > 0 {
		message.Add(stun.AttrUsername, []byte(token))
	}
	// 创建用于接收异步结果的 channel
	resChan := make(chan stun.Event, 1)
	// 2. 设置 1 秒超时 Context
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// 3. 发送请求并监听 STUN 服务器的响应
	if err := cli.Start(message, func(res stun.Event) {
		resChan <- res
	}); err != nil {
		return err
	}
	// 2. 阻塞等待：要么收到回调响应，要么 Context 超时/取消
	select {
	case res := <-resChan:
		if res.Error != nil {
			slog.Error("STUN 响应错误: ", slog.Any("err", res.Error))
			return res.Error
		}
		if err = c.ExternalAddress.GetFrom(res.Message); err != nil {
			slog.Error("解析 XOR-MAPPED-ADDRESS 失败:  ", slog.Any("err", err))
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) OnClosing() bool {
	slog.Debug("正在断开stun连接...")
	return true
}

func (c *Client) OnClosed() {
	slog.Debug("stun已经断开！")
}
