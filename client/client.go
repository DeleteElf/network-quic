package client

import "C"
import (
	"context"
	"errors"
	"github.com/DeleteElf/zero-net/framework"
	"github.com/DeleteElf/zero-net/framework/network"
	"github.com/DeleteElf/zero-net/framework/utils"
	"github.com/DeleteElf/zero-net/ice"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlogwriter"
	"log/slog"
	"net"
	"time"
)

// Client 客户端
type Client struct {
	Id        string
	SessionId string
	//需要连接的服务端地址
	ServerAddress string
	serverAddr    net.Addr

	NetConn       net.PacketConn
	QuicConn      *quic.Conn
	Socket        *network.Socket
	StreamConfigs []network.StreamConfig
	network.Config
	ice.IceWorker
	framework.CloseableObject
}

// NewClient 创建客户端实例
func NewClient(addr string, id string) *Client {
	cli := &Client{
		ServerAddress: addr,
		Id:            id,
	}
	cli.SetOnCloseHandler(cli)
	return cli
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
		_ = cli.Socket.Close()
		cli.Socket = nil
		slog.Debug("客户端的socket已关闭！")
	}
	if cli.NetConn != nil {
		_ = cli.NetConn.Close()
	}
	return true
}

func (cli *Client) OnClosed() error {
	slog.Debug("客户端已经关闭")
	return nil
}

// ConnectByIce 通过Ice连接
//
//	-param conn:通过Ice获取到的连接
//	-param dummyAddr:通过ice获取的连接的目标地址，注意：因为使用的是已知打通的 PacketConn，Target Address 可以使用 Dummy 虚拟地址
//
// return: 返回错误
func (cli *Client) ConnectByIce(conn net.PacketConn, dummyAddr net.Addr) error {
	cli.NetConn = conn
	return cli.ConnectToNet(3, nil, dummyAddr, func(sock *network.Socket) {
		slog.Debug("socket已经断开===》！", slog.String("id", sock.Id))
	})
}

func (cli *Client) Connect(channelCount int, networkType string, onDisconnect network.SocketCallbackFunc) error {
	if networkType != network.STREAM_NETWORK_UDP {
		return errors.New("暂时只支持udp连接！")
	}
	var err error
	netConn, err := network.NewUdpSocketClient()
	if err != nil {
		slog.Error("创建UDP客户端失败", slog.Any("err", err))
		_ = cli.Close()
		return err
	}
	netAddr, err := net.ResolveUDPAddr(network.STREAM_NETWORK_UDP, cli.ServerAddress)
	if err != nil {
		slog.Error("解析服务端地址失败", slog.Any("err", err))
		return err
	}
	return cli.ConnectToNet(channelCount, netConn, netAddr, onDisconnect)
}
func (cli *Client) ConnectToNet(channelCount int, conn net.PacketConn, addr net.Addr, onDisconnect network.SocketCallbackFunc) error {
	if cli.Socket != nil {
		return errors.New("当前客户端已经连接！")
	}
	if conn != nil {
		cli.NetConn = conn
	}
	cli.serverAddr = addr

	tlsConfig := utils.GenTLSConfig()
	if cli.QuicConfig == nil {
		cli.QuicConfig = &quic.Config{
			//MaxIncomingStreams:      0xffffffffffff,   // 最大默认stream输入，默认100
			HandshakeIdleTimeout:    5 * time.Second,  // 默认5s
			MaxIdleTimeout:          10 * time.Second, // 默认30s，我们这边设置成10秒
			KeepAlivePeriod:         3 * time.Second,  // 建议是 MaxIdleTimeout 的一半，或者更小的值
			InitialPacketSize:       1400,             //当前最大数据包一个基础包的大小
			DisablePathMTUDiscovery: false,
			Allow0RTT:               true,
			EnableDatagrams:         cli.SupportFec,
			Tracer: func(ctx context.Context, isClient bool, connID quic.ConnectionID) qlogwriter.Trace {
				ctrl := &network.NetStatusControl{ShowStatusLevel: network.StatusLevelLostPacket}
				return network.NewNetStatusTracer(ctrl)
			},
		}
	}
	slog.Debug("正在远程连接", slog.Any("ServerAddress", cli.serverAddr))
	tr := &quic.Transport{
		Conn: cli.NetConn,
	}
	quicConn, err := tr.Dial(context.Background(), cli.serverAddr, tlsConfig, cli.QuicConfig)
	if err != nil {
		slog.Info("远程连接失败！", slog.Any("err", err))
		return err
	}
	if cli.StreamConfigs == nil { //如果没有配置，则默认生成配置
		cli.StreamConfigs = make([]network.StreamConfig, channelCount)
	}
	if cli.SupportFec { //如果启动了Fec，我们需要对fec的配置进行检查
		for i := 0; i < channelCount; i++ {
			if i >= 3 { //如果太多条的流，外部没有明确，则后面都是属于视频通道流
				cli.StreamConfigs[i].Type = network.Video
			} else {
				cli.StreamConfigs[i].Type = network.StreamType(i)
			}
			switch cli.StreamConfigs[i].Type {
			case network.Video: //暂时内置参数
				cli.StreamConfigs[i].DataShards = 10
				cli.StreamConfigs[i].ParityShards = 3
				cli.StreamConfigs[i].EnableFec = true
				break
			case network.Audio: //暂时内置参数
				cli.StreamConfigs[i].DataShards = 4
				cli.StreamConfigs[i].ParityShards = 2
				cli.StreamConfigs[i].EnableFec = true
				break
			default:
				break
			}
		}
	}
	cli.Socket = network.NewSocket(cli.Id, channelCount, onDisconnect)
	cli.Socket.StreamConfigs = cli.StreamConfigs
	cli.Socket.CreateChannels()
	cli.Socket.Conn = quicConn

	slog.Info("客户端连接成功！", slog.Int("通道数", cli.Socket.ChannelCount))
	for i := 0; i < channelCount; i++ {
		err = cli.Socket.InitFecParam(i)
		if err != nil {
			return err
		}

		info := network.StreamInfo{
			Id:           cli.Id,
			ChannelCount: channelCount,
			Ts:           time.Now().Unix(),
			ChannelIndex: i,
			Type:         int(cli.StreamConfigs[i].Type), //这里需要告诉服务端，是什么类型的流
			DataShards:   cli.StreamConfigs[i].DataShards,
			ParityShards: cli.StreamConfigs[i].ParityShards,
		}
		stream, err := network.CreateStream(cli.Socket.Conn, info) //创建并打开流
		if err != nil {
			_ = cli.Close()
			return err
		}
		go cli.Socket.HandleChannelStreamData(i, stream)
	}
	if cli.QuicConfig.EnableDatagrams && cli.SupportFec {
		if cli.Socket.PacketPool == nil {
			cli.Socket.PacketPool = cli.Socket.CreatePacketPool(cli.QuicConfig.InitialPacketSize)
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
