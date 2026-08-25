package network

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/DeleteElf/zero-net/framework"
	"github.com/DeleteElf/zero-net/framework/utils"
	"github.com/klauspost/reedsolomon"
	"github.com/quic-go/quic-go"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// FECGroup 用于收集和组装同一 GroupID 的分片
type FECGroup struct {
	GroupID      uint64
	DataShards   int
	ParityShards int
	Total        int
	Shards       [][]byte  // 槽位数组，长度为 DataShards + ParityShards
	Received     int       // 当前已收到的有效分片数
	CreatedAt    time.Time // 创建时间，用于过期清理
}

type StreamChannelOperating interface {
	CreateChannels(count int)
	HandleChannelStreamData(channel chan StreamChannelData, channelId int, stream *quic.Stream)
	Send(channelId int, data []byte) (bool, error)
}

// StreamChannelData 流通道数据结构
type StreamChannelData struct {
	ClientId  string
	ChannelId int
	Offset    int
	Data      []byte
}

type MessageChannelCallbackFunc func(string, int)

type StreamChannel struct {
	Channel       chan StreamChannelData
	ClientId      string
	ChannelId     int
	Cancel        context.CancelFunc
	Done          bool
	Buffer        *StreamChannelData
	Stream        *quic.Stream
	FrameIndex    uint64
	FecEncoders   map[string]reedsolomon.Encoder
	FecGroups     map[uint64]*FECGroup
	SharedShards  [][]byte
	NextGroupId   uint64
	lockEncoders  sync.Mutex
	lockFecGroups sync.Mutex

	OnConnect    MessageChannelCallbackFunc
	OnDisconnect MessageChannelCallbackFunc

	framework.CloseableObject
}

// NewStreamChannel 创新数据通道，并确定传输类型
//
//	-param id:数据通道的编号
//	-param index:数据通道的索引
//	-param c:数据通道的配置
//
// return:通道实例
func NewStreamChannel(id string, index int) *StreamChannel {
	slog.Debug("正在创建通道", slog.String("id", id), slog.Int("ChannelId", index))
	sc := &StreamChannel{
		Channel:     make(chan StreamChannelData),
		ClientId:    id,
		ChannelId:   index,
		FecGroups:   make(map[uint64]*FECGroup), //初始化空的分组队列
		FecEncoders: make(map[string]reedsolomon.Encoder),
		CloseableObject: framework.CloseableObject{
			IsClosed: false,
		},
	}
	sc.SetOnCloseHandler(sc)
	return sc
}

func (sc *StreamChannel) OnClosing() bool {
	if sc.Cancel != nil {
		sc.Cancel()
	}
	sc.Cancel = nil
	if sc.Stream != nil {
		sc.Stream.CancelRead(0)
		_ = sc.Stream.Close()
		sc.Stream.CancelWrite(0)
		sc.Stream = nil
	}
	count := 100
	for i := 0; i < count; i++ {
		if !sc.Done {
			time.Sleep(time.Millisecond)
			continue
		}
		break
	}
	return true
}

func (sc *StreamChannel) OnClosed() error {
	slog.Debug("检测到通道已经退出！", slog.String("id", sc.ClientId), slog.Int("通道", sc.ChannelId))
	sc.Buffer = nil
	return nil
}

// HandleChannelStreamData 从通道接收流的数据
func (sc *StreamChannel) HandleChannelStreamData(stream *quic.Stream) {
	sc.Stream = stream
	_, sc.Cancel = context.WithCancel(sc.Stream.Context())
	defer func() {
		if !sc.Done && sc.Channel != nil {
			close(sc.Channel)
			sc.Channel = nil
		}
		sc.Done = true
		if sc.OnDisconnect != nil {
			sc.OnDisconnect(sc.ClientId, sc.ChannelId)
		}
	}()
	slog.Debug("完成流与通道的对接，开始读取通道数据", slog.Int("channel", sc.ChannelId))
	if sc.OnConnect != nil {
		sc.OnConnect(sc.ClientId, sc.ChannelId)
	}
	for {
		if sc.IsClosed {
			return
		}
		buf, err := utils.ReadStreamByHeaderUShort(sc.Stream)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) { //如果是读取超时，我们就继续即可
				continue
			} else if err != io.EOF {
				//信息太频繁，不用一直提示
				//slog.Error("通道读取失败！", slog.Int("ChannelId", sc.ChannelId), slog.Any("err", err))
			} else {
				//slog.Error("通道流已经结束！", slog.Int("ChannelId", sc.ChannelId))
			}
			return
		}
		if sc.IsClosed {
			return
		}
		if len(buf) == 0 { //读取到0长度的数据包，我们认为是断开连接了
			return
		}
		if sc == nil {
			return
		}
		if sc.Channel == nil {
			return
		}
		sc.Channel <- StreamChannelData{
			ClientId:  sc.ClientId,
			ChannelId: sc.ChannelId,
			Offset:    0,
			Data:      buf,
		}
	}
}

func (sc *StreamChannel) ReceiveDataToBuffer() bool {
	if sc.Buffer == nil { //当前缓存没有工作时
		buffer, ok := <-sc.Channel
		if !ok {
			//slog.Warn("通道已经关闭！")
			return ok
		}
		sc.Buffer = &buffer
	}
	return true
}

func (sc *StreamChannel) Send(data []byte) (bool, error) {
	if sc.IsClosed {
		return false, nil
	}
	if sc.Stream == nil {
		return false, nil
	}
	err := utils.WriteStreamByHeaderUShort(sc.Stream, data)
	return err == nil, err
}

func (sc *StreamChannel) FecDecode(packet *FECPacket) error {
	totalShards := packet.DataShards + packet.ParityShards
	if packet.ShardIdx >= totalShards {
		return fmt.Errorf("无效的shard索引: %d", packet.ShardIdx)
	}
	group, exists := sc.FecGroups[packet.GroupId]
	if !exists {
		group = &FECGroup{
			GroupID: packet.GroupId, DataShards: packet.DataShards, ParityShards: packet.ParityShards,
			Shards: make([][]byte, totalShards), Total: packet.Total,
			Received: 0, CreatedAt: time.Now(),
		}
		sc.FecGroups[packet.GroupId] = group
	}
	// 3. 去重与填槽
	if group.Shards[packet.ShardIdx] == nil {
		group.Shards[packet.ShardIdx] = packet.Payload
		group.Received++
	}
	//slog.Debug("收到一个新的数据包", slog.Any("packet", packet), slog.Any("fecGroup", group))
	// 4. 判定：如果不满足解包门槛，继续等待下一个包
	for {
		next, exists := sc.FecGroups[sc.NextGroupId]
		if exists && next.Received >= next.DataShards {
			// 关键优化：使用 ReconstructData 仅恢复数据分片，比 Reconstruct 省时省 CPU
			encoder, err := sc.GetFecEncoder(next.DataShards, next.ParityShards)
			if err != nil {
				return fmt.Errorf("获取fec解码器出错: %w", err)
			}
			err = encoder.ReconstructData(next.Shards)
			if err != nil {
				return fmt.Errorf("fec解码出错: %w", err)
			}
			var frameBuf bytes.Buffer
			err = encoder.Join(&frameBuf, next.Shards, next.Total)
			if err != nil {
				return err
			}
			//slog.Debug("解码fec完成", slog.Int("groupId", int(next.GroupID)))
			delete(sc.FecGroups, next.GroupID)
			atomic.AddUint64(&sc.NextGroupId, 1)
			if sc.Channel != nil {
				sc.Channel <- StreamChannelData{
					ClientId:  sc.ClientId,
					ChannelId: sc.ChannelId,
					Offset:    0,
					Data:      frameBuf.Bytes(),
				}
			}
		} else {
			return nil
		}
	}
}

func (sc *StreamChannel) GetFecEncoder(dataShards, parityShards int) (reedsolomon.Encoder, error) {
	if dataShards > 0 && parityShards > 0 {
		key := fmt.Sprintf("%d_%d", dataShards, parityShards)
		sc.lockEncoders.Lock()
		defer sc.lockEncoders.Unlock()
		if sc.FecEncoders[key] == nil {
			encoder, err := reedsolomon.New(dataShards, parityShards) //todo:如果需要动态调整时，整理会进行实时修改，如何保证修改前和修改后的发送不会出错？
			if err != nil {
				return nil, err
			}
			sc.FecEncoders[key] = encoder
		}
		return sc.FecEncoders[key], nil
	}
	return nil, nil
}
