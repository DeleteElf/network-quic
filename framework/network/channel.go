package network

import (
	"bytes"
	"context"
	"encoding/binary"
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

type FecGroupsMap struct {
	Groups      map[uint64]*FecGroup //仅用于解码
	NextGroupId uint64               //仅用于解码
	FrameIndex  uint64               //仅用于编码
}

func NewFecGroupsMap() *FecGroupsMap {
	return &FecGroupsMap{
		Groups:      make(map[uint64]*FecGroup),
		NextGroupId: 0,
		FrameIndex:  0,
	}
}

// FecPacket Fec数据包,包头长度 12字节
type FecPacket struct {
	//通道的编号，主要用于路由分发，1个字节
	ChannelId int
	Ssrc      uint32
	//分组的编号，主要用于重组，8个字节，考虑到可能会播放很久,GroupId并不等于FrameIndex，一个数据包可能被拆成多个分组
	GroupId uint64
	Idr     bool
	//shard的索引 1个字节
	ShardIdx int
	//原始数据长度 2个字节 暂时不知道有没意义。。。
	//DataLength uint16
	//核心数据分片数
	DataShards int
	//fec矩阵分片数
	ParityShards int
	//数据总长度
	Total int
	//数据包体长度 2个字节
	Length  uint16
	Payload []byte
}

// FecGroup 用于收集和组装同一 GroupID 的分片
type FecGroup struct {
	GroupID        uint64
	DataShards     int
	ParityShards   int
	Total          int
	Shards         [][]byte // 槽位数组，长度为 DataShards + ParityShards
	Packets        []*FecPacket
	HeaderTemplate []byte
	Received       int       // 当前已收到的有效分片数
	CreatedAt      time.Time // 创建时间，用于过期清理
	ExpiredAt      time.Time
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
	Channel   chan StreamChannelData
	ClientId  string
	ChannelId int
	Cancel    context.CancelFunc
	Done      bool
	Buffer    *StreamChannelData
	Stream    *quic.Stream
	//FrameIndex    uint64
	FecEncoders  map[string]reedsolomon.Encoder
	FecGroups    map[uint32]*FecGroupsMap
	SharedShards [][]byte
	ParityShards [][]byte
	//NextGroupId   uint64
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
	cacheCount := 2
	if index >= 2 {
		cacheCount = 60
	}
	sc := &StreamChannel{
		Channel:     make(chan StreamChannelData, cacheCount),
		ClientId:    id,
		ChannelId:   index,
		FecGroups:   make(map[uint32]*FecGroupsMap), //初始化空的分组队列
		FecEncoders: make(map[string]reedsolomon.Encoder),
		CloseableObject: framework.CloseableObject{
			IsClosed: false,
		},
	}
	sc.FecGroups[0] = NewFecGroupsMap() //初始化一个组
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

func (sc *StreamChannel) FecDecode(packet *FecPacket) error {
	totalShards := packet.DataShards + packet.ParityShards
	if packet.ShardIdx >= totalShards {
		return fmt.Errorf("无效的shard索引: %d", packet.ShardIdx)
	}
	g, exists := sc.FecGroups[packet.Ssrc]
	if !exists {
		g = NewFecGroupsMap()
		sc.FecGroups[packet.Ssrc] = g
		atomic.AddUint64(&g.NextGroupId, 1)
	}
	group, exists := g.Groups[packet.GroupId]
	isRtp := (packet.Payload[0] & 0x80) == 0x80
	if !exists {
		group = &FecGroup{
			GroupID: packet.GroupId, DataShards: packet.DataShards, ParityShards: packet.ParityShards,
			Shards: make([][]byte, totalShards), Packets: make([]*FecPacket, totalShards),
			Total: packet.Total, Received: 0, CreatedAt: time.Now(), ExpiredAt: time.Now().Add(50 * time.Millisecond),
		}
		if isRtp { //如果判定是rtp包，我们就需要预处理一下数据，方便后期补充rtp包
			switch packet.Payload[1] {
			case 0x61: //标准音频
				group.HeaderTemplate = packet.Payload[:RtpHeaderLength]
			case 0x7f: //动态音频
				group.HeaderTemplate = packet.Payload[:AudioHeaderLength]
			default: //其他都是视频 视频数据的rtp包数据都是一样的
				group.HeaderTemplate = packet.Payload[:VideoHeaderLength]
			}
		}
		sc.FecGroups[packet.Ssrc].Groups[packet.GroupId] = group
	}
	if group.Shards[packet.ShardIdx] == nil {
		group.Packets[packet.ShardIdx] = packet // 记录原始包指针
		if isRtp {                              //如果是rtp包
			switch packet.Payload[1] { //packetType
			case 97:
				group.Shards[packet.ShardIdx] = packet.Payload[RtpHeaderLength:]
			case 127:
				group.Shards[packet.ShardIdx] = packet.Payload[AudioHeaderLength:]
			default: //video
				group.Shards[packet.ShardIdx] = packet.Payload[VideoHeaderLength:]
			}
		} else {
			group.Shards[packet.ShardIdx] = packet.Payload
		}
		group.Received++
	}
	//slog.Debug("收到一个新的数据包", slog.Any("packet", packet), slog.Any("fecGroup", group))
	// 4. 判定：如果不满足解包门槛，继续等待下一个包
	for {
		next, exists := g.Groups[g.NextGroupId]
		if !exists {
			break // 下一个组还没到来，退出循环
		}
		// 如果当前等待的组包数量还不足以解码，直接中断等待下一个网络包到达，切勿死循环！
		if next.Received < next.DataShards {
			if next.ExpiredAt.Before(time.Now()) { //如果已经过期，则不再等待，直接接收下一个
				if sc.ChannelId == 2 {
					slog.Debug("帧接收超时丢弃！", slog.Int("channel", sc.ChannelId), slog.Any("groupId", g.NextGroupId))
				}
				delete(g.Groups, g.NextGroupId)
				g.NextGroupId++ // 单协程处理下无需 atomic，若多协程则整体加锁
				continue        //过期了，不论是否是关键帧，我们都丢弃了，那么还需要继续等待下一个
			}
			if isRtp && packet.Idr { //如果是rtp数据包，我们需要检查一下
				if packet.GroupId > g.NextGroupId {
					if sc.ChannelId == 2 {
						slog.Debug("帧接收新的关键帧，跳到！", slog.Int("channel", sc.ChannelId), slog.Any("groupId", packet.GroupId))
					}
					for i := g.NextGroupId; i < packet.GroupId; i++ { //循环删除当前帧之前的数据
						delete(g.Groups, i)
						slog.Debug("清除到下个关键帧直接的缓存数据，快速追帧！", slog.Any("groupId", i))
					}
					g.NextGroupId = packet.GroupId //直接移动到当前帧
					continue
				}
			}
			break
		}
		// 关键优化：使用 ReconstructData 仅恢复数据分片，比 Reconstruct 省时省 CPU
		encoder, err := sc.GetFecEncoder(next.DataShards, next.ParityShards)
		if err != nil {
			return fmt.Errorf("获取fec解码器出错: %w", err)
		}
		err = encoder.ReconstructData(next.Shards)
		if err != nil {
			return fmt.Errorf("fec解码出错: %w", err)
		}
		//slog.Debug("解码fec完成", slog.Int("groupId", int(next.GroupID)))
		delete(g.Groups, next.GroupID)
		g.NextGroupId++ // 单协程处理下无需 atomic，若多协程则整体加锁
		//if sc.ChannelId == 2 {
		//	slog.Debug("解码成功！", slog.Int("channel", sc.ChannelId), slog.Any("ssrc", packet.Ssrc))
		//}
		if isRtp { //如果是rtp包
			//这里可以根据特性进行拼接数据,如果考虑尽量零拷贝处理next.Shards
			//现在这里有几个问题：
			//1，我需要补充没有到的正规rtp 包的头信息 ，假设 一共6个数据包，数据分片是4个，当前到达的索引是 0,1,3,4，那么则需要补充序号是2的rtp头
			//2，我需要告诉上层逻辑，fec已经处理完毕了
			isVideo := next.Packets[0].Payload[1] != 0x61 && next.Packets[0].Payload[1] != 0x7f
			for i := 0; i < next.DataShards; i++ {
				var resultData []byte
				if next.Packets[i] == nil { //Payload 是携带rtp包头信息的完整数据缓存
					//todo:这里重新构建缺失的头，那么取的数据可能是其他任意数据的头，因为，我们不知道是哪个
					resultData = RebuildRtpPacket(next.HeaderTemplate, next.Shards[i], uint16(i), uint16(next.DataShards))
				} else { //清除fecPercentage的数据
					resultData = next.Packets[i].Payload //直接使用原始数据包，实现零拷贝
				}
				if isVideo { //因为我们已经处理过fec了，必须告诉上层没有fec分片数据了
					oldFecInfo := binary.LittleEndian.Uint32(resultData[28:])
					binary.LittleEndian.PutUint32(resultData[28:32], oldFecInfo&^(0x7F<<4))
					binary.LittleEndian.PutUint32(resultData[16:], uint32(binary.BigEndian.Uint16(resultData[2:]))<<8)
				}

				if sc.Channel != nil {
					sc.Channel <- StreamChannelData{
						ClientId:  sc.ClientId,
						ChannelId: sc.ChannelId,
						Offset:    0,
						Data:      resultData, //直接使用原始数据包，实现零拷贝
					}
				}
			}
		} else {
			var frameBuf bytes.Buffer
			err = encoder.Join(&frameBuf, next.Shards, next.Total)
			if err != nil {
				return err
			}
			//slog.Debug("fec重组了一条数据", slog.Any("frame", frameBuf.String()))
			if sc.Channel != nil {
				sc.Channel <- StreamChannelData{
					ClientId:  sc.ClientId,
					ChannelId: sc.ChannelId,
					Offset:    0,
					Data:      frameBuf.Bytes(),
				}
			}
		}
	}
	return nil
}

func (sc *StreamChannel) GetFecEncoder(dataShards, parityShards int) (reedsolomon.Encoder, error) {
	if dataShards > 0 && parityShards > 0 {
		key := fmt.Sprintf("%d_%d", dataShards, parityShards)
		sc.lockEncoders.Lock()
		defer sc.lockEncoders.Unlock()
		if sc.FecEncoders[key] == nil {
			encoder, err := reedsolomon.New(dataShards, parityShards)
			if err != nil {
				return nil, err
			}
			sc.FecEncoders[key] = encoder
		}
		return sc.FecEncoders[key], nil
	}
	return nil, nil
}
