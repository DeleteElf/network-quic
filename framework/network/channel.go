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
	"math"
	"sync"
	"time"
)

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
	FecEncoders   map[string]reedsolomon.Encoder
	FecGroups     map[uint8]*FecGroupsMap
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
	//cacheCount := 2
	//if index >= 2 {
	//	cacheCount = 60
	//}
	sc := &StreamChannel{
		Channel:     make(chan StreamChannelData),
		ClientId:    id,
		ChannelId:   index,
		FecGroups:   make(map[uint8]*FecGroupsMap), //初始化空的分组队列
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

func (sc *StreamChannel) CheckTimeout(group *FecGroup, groups *FecGroupsMap) bool {
	if group.ExpiredAt.Before(time.Now()) { //如果已经过期，则不再等待，直接接收下一个
		slog.Debug("帧接收超时丢弃！", slog.Int("channel", sc.ChannelId), slog.Any("groupId", groups.NextGroupId))
		delete(groups.Groups, groups.NextGroupId)
		groups.NextGroupId++ // 单协程处理下无需 atomic，若多协程则整体加锁
		//continue        //过期了，不论是否是关键帧，我们都丢弃了，那么还需要继续等待下一个
		return true
	}
	return false
}

func (sc *StreamChannel) FecDecode(packet *FecPacket) error {
	//slog.Debug("fec开始解码", slog.Any("channel id", sc.ChannelId), slog.Any("ssrc", packet.Header.Ssrc), slog.Any("groupId", packet.Header.GroupId))
	totalShards := packet.Header.DataShards + packet.Header.ParityShards
	if packet.Header.ShardIdx >= totalShards {
		return fmt.Errorf("无效的shard索引: %d", packet.Header.ShardIdx)
	}
	g, exists := sc.FecGroups[packet.Header.Ssrc]
	if !exists {
		g = NewFecGroupsMap()
		sc.FecGroups[packet.Header.Ssrc] = g
	}
	group, exists := g.Groups[packet.Header.GroupId]
	isRtp := packet.Payload[0] == RtpHeader || packet.Payload[0] == VideoHeader
	if !exists {
		group = &FecGroup{
			HeaderSample: &packet.Header, ExpiredAt: time.Now().Add(200 * time.Millisecond),
			Shards: make([][]byte, totalShards), Packets: make([]*FecPacket, totalShards),
			//GroupID: packet.Header.GroupId, DataShards: packet.Header.DataShards, ParityShards: packet.Header.ParityShards,
			//Total: packet.Header.Total, Received: 0, CreatedAt: time.Now(),
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
		} else {
			//group.HeaderTemplate = packet.Payload[:FecPacketHeaderLength] //因为信息一致性，我们其实不用再次赋值
		}
		sc.FecGroups[packet.Header.Ssrc].Groups[packet.Header.GroupId] = group
	}
	if group.Shards[packet.Header.ShardIdx] == nil {
		if isRtp { //如果是rtp包
			switch packet.Payload[1] { //packetType
			case 97:
				group.Shards[packet.Header.ShardIdx] = packet.Payload[RtpHeaderLength:]
			case 127:
				group.Shards[packet.Header.ShardIdx] = packet.Payload[AudioHeaderLength:]
			default: //video
				group.Shards[packet.Header.ShardIdx] = packet.Payload[VideoHeaderLength:]
			}
		} else {
			group.Shards[packet.Header.ShardIdx] = packet.Payload[FecPacketHeaderLength:]
		}
		group.Packets[packet.Header.ShardIdx] = packet // 记录原始包指针
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
		header := next.HeaderSample //连续组装，不能使用packet，而应该从当前分组取样本
		if next.Received < header.DataShards {
			if sc.CheckTimeout(next, g) {
				continue
			}
			if isRtp && header.Idr == 1 { //如果是rtp数据包，我们需要检查一下
				if utils.IsBefore8(g.NextGroupId, header.GroupId) {
					slog.Debug("帧接收新的关键帧，跳到！", slog.Int("channel", sc.ChannelId),
						slog.Any("groupId", header.GroupId))
					target := int(header.GroupId)
					if header.GroupId < g.NextGroupId {
						target = int(header.GroupId) + math.MaxUint8
					}
					for i := int(g.NextGroupId); i < target; i++ { //循环删除当前帧之前的数据
						delete(g.Groups, uint8(i))
					}
					g.NextGroupId = header.GroupId //直接移动到当前帧
					continue
				}
			}
			break
		}
		// 关键优化：使用 ReconstructData 仅恢复数据分片，比 Reconstruct 省时省 CPU
		encoder, err := sc.GetFecEncoder(header.DataShards, header.ParityShards)
		if err != nil {
			if sc.CheckTimeout(next, g) {
				continue
			}
			return fmt.Errorf("获取fec解码器出错【%d】: %w", header.GroupId, err)
		}
		err = encoder.ReconstructData(next.Shards)
		if err != nil {
			if sc.CheckTimeout(next, g) {
				continue
			}
			return fmt.Errorf("fec解码出错【%d】:  %w", header.GroupId, err)
		}
		//slog.Debug("解码fec完成", slog.Int("groupId", int(next.GroupID)))
		delete(g.Groups, header.GroupId)
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
			//slog.Debug("fec开始重组", slog.Any("channel id", sc.ChannelId), slog.Any("groupId", header.GroupId))
			for i := 0; i < int(header.DataShards); i++ {
				var resultData []byte
				if next.Packets[i] == nil { //Payload 是携带rtp包头信息的完整数据缓存
					resultData = RebuildRtpPacket(next.HeaderTemplate, next.Shards[i], uint8(i), header.DataShards)
				} else { //清除fecPercentage的数据
					resultData = next.Packets[i].Payload //直接使用原始数据包，实现零拷贝
				}
				if isVideo { //因为我们已经处理过fec了，必须告诉上层没有fec分片数据了
					oldFecInfo := binary.LittleEndian.Uint32(resultData[28:])
					binary.LittleEndian.PutUint32(resultData[28:32], oldFecInfo&^(0x7F<<4))
				}
				//slog.Debug("fec重组了一条数据", slog.Any("channel id", sc.ChannelId), slog.Any("shard index", i))
				if sc.Channel != nil {
					sc.Channel <- StreamChannelData{
						ClientId:  sc.ClientId,
						ChannelId: sc.ChannelId,
						Offset:    0,
						Data:      resultData, //直接使用原始数据包，实现零拷贝
					}
				}
			}
			//slog.Debug("fec完成重组", slog.Any("channel id", sc.ChannelId), slog.Any("groupId", header.GroupId))
		} else {
			//slog.Debug("fec收到意料外的数据", slog.Any("channel id", sc.ChannelId))
			var frameBuf bytes.Buffer
			err = encoder.Join(&frameBuf, next.Shards, int(header.Total))
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

func (sc *StreamChannel) GetFecEncoder(dataShards, parityShards uint8) (reedsolomon.Encoder, error) {
	if dataShards > 0 && parityShards > 0 {
		key := fmt.Sprintf("%d_%d", dataShards, parityShards)
		sc.lockEncoders.Lock()
		defer sc.lockEncoders.Unlock()
		if sc.FecEncoders[key] == nil {
			encoder, err := reedsolomon.New(int(dataShards), int(parityShards))
			if err != nil {
				return nil, err
			}
			sc.FecEncoders[key] = encoder
		}
		return sc.FecEncoders[key], nil
	}
	return nil, nil
}
