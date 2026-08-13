package client

import "C"
import (
	"context"
	"errors"
	"github.com/DeleteElf/zero-net/framework"
	"github.com/DeleteElf/zero-net/framework/streams"
	"github.com/DeleteElf/zero-net/framework/utils"
	"github.com/DeleteElf/zero-net/stunhelper"
	"github.com/quic-go/quic-go"
	"log/slog"
	"net"
	"time"
)

// Client 客户端
type Client struct {
	Id string
	//stun服务地址
	Stun string
	//外网地址
	ExternalIp   string
	ExternalPort int
	//需要连接的服务端地址
	ServerAddress string
	NetConn       net.PacketConn
	netAddr       net.Addr
	//quicConn      *quic.Conn
	Socket *streams.Socket
	framework.CloseableObject
}

// NewClient 创建客户端实例
func NewClient(addr string, id string) *Client {
	cli := &Client{
		ServerAddress: addr,
		Id:            id,
	}
	cli.IsClosed = false
	cli.SetOnCloseHandler(cli)
	return cli
}
func (cli *Client) DetectStun(token string) {
	if len(cli.Stun) > 0 {
		slog.Debug("配置了stun服务，正在准备探测！", slog.String("address", cli.Stun))
		client := stunhelper.NewClient()
		err := client.Connect(cli.Stun, token, cli.NetConn)
		if err == nil {
			slog.Debug("你的公网 IP 地址 :", slog.Any("ip", client.ExternalAddress.IP))
			slog.Debug("你的公网映射端口 : ", slog.Int("port", client.ExternalAddress.Port))
			cli.ExternalIp = client.ExternalAddress.IP.String()
			cli.ExternalPort = client.ExternalAddress.Port
		}
		client.Close()
	}
}

func (cli *Client) CloseChannel(channelId int) bool {
	if !cli.IsClosed && cli.Socket != nil {
		return cli.Socket.CloseChannel(channelId)
	}
	return false
}
func (cli *Client) OnClosing() bool {
	if cli.Socket != nil {
		slog.Debug("正在关闭客户端的socket！")
		cli.Socket.Close()
		cli.Socket = nil
		slog.Debug("客户端的socket已关闭！")
	}
	if cli.NetConn != nil {
		_ = cli.NetConn.Close()
	}
	return true
}

func (cli *Client) OnClosed() {
	slog.Debug("客户端已经关闭")
}

func (cli *Client) Connect(channelCount int, networkType string, onDisconnect streams.SocketCallbackFunc) error {
	if networkType != "udp" {
		return errors.New("暂时只支持udp连接！")
	}
	var err error
	netConn, err := streams.NewUdpSocketClient()
	if err != nil {
		slog.Error("创建UDP客户端失败", slog.Any("err", err))
		cli.Close()
		return err
	}
	netAddr, err := net.ResolveUDPAddr(streams.STREAM_NETWORK_UDP, cli.ServerAddress)
	if err != nil {
		slog.Error("解析服务端地址失败", slog.Any("err", err))
		return err
	}
	return cli.ConnectToNet(channelCount, netConn, netAddr, onDisconnect)
}
func (cli *Client) ConnectToNet(channelCount int, conn net.PacketConn, addr net.Addr, onDisconnect streams.SocketCallbackFunc) error {
	if cli.Socket != nil {
		return errors.New("当前客户端已经连接！")
	}
	cli.NetConn = conn
	cli.netAddr = addr

	tlsConfig := utils.GenTLSConfig()
	quicConfig := &quic.Config{
		MaxIncomingStreams:      0xffffffffffff,   // 最大默认stream输入，默认100
		HandshakeIdleTimeout:    5 * time.Second,  // 默认5s
		MaxIdleTimeout:          10 * time.Second, // 默认30s，我们这边设置成10秒
		KeepAlivePeriod:         3 * time.Second,  // 建议是 MaxIdleTimeout 的一半，或者更小的值
		InitialPacketSize:       1500,             //当前最大数据包一个基础包的大小
		DisablePathMTUDiscovery: false,
		Allow0RTT:               true,
		EnableDatagrams:         true,
	}
	slog.Debug("正在远程连接", slog.Any("ServerAddress", cli.netAddr))
	quicConn, err := quic.Dial(context.TODO(), cli.NetConn, cli.netAddr, tlsConfig, quicConfig)
	if err != nil {
		slog.Info("远程连接失败！", slog.Any("err", err))
		return err
	}
	cli.Socket = streams.NewSocket(cli.Id, channelCount, onDisconnect)
	cli.Socket.Conn = quicConn
	if cli.Socket.ChannelCount == 4 { //如果创建4个流，我们第4个流也是视频流，目前的版本暂时只有3个流
		cli.Socket.StreamChannels[3].Type = streams.Video
	}

	slog.Info("客户端连接成功！", slog.Int("通道数", cli.Socket.ChannelCount))
	for i := 0; i < channelCount; i++ {
		info := streams.StreamInfo{
			Id:    cli.Id,
			Count: channelCount,
			Ts:    time.Now().Unix(),
			Index: i,
			Type:  int(cli.Socket.StreamChannels[i].Type), //这里需要告诉服务端，是什么类型的流
		}
		switch cli.Socket.StreamChannels[i].Type {
		case streams.Video: //暂时内置参数
			info.DataShards = 10
			info.ParityShards = 3
			cli.Socket.SetFecParam(i, info.DataShards, info.ParityShards)
			break
		case streams.Audio: //暂时内置参数
			info.DataShards = 4
			info.ParityShards = 2
			cli.Socket.SetFecParam(i, info.DataShards, info.ParityShards)
			break
		default:
			break
		}

		stream, err := streams.CreateStream(cli.Socket.Conn, info) //创建并打开流
		if err != nil {
			cli.Close()
			return err
		}
		go cli.Socket.HandleChannelStreamData(i, stream)
	}
	if quicConfig.EnableDatagrams {
		if cli.Socket.PacketPool == nil {
			cli.Socket.PacketPool = cli.Socket.CreatePacketPool(quicConfig.InitialPacketSize)
		}

		go cli.Socket.HandleChannelStreamDatagram()
	}
	return nil
}

func (cli *Client) Send(channleId int, data []byte) (bool, error) {
	if cli.IsClosed {
		return false, errors.New("client is closed")
	}
	if cli.Socket == nil {
		return false, errors.New("socket is null")
	}
	return cli.Socket.Send(channleId, data)

}
