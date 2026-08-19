package network

import (
	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
	"log/slog"
	"sync/atomic"
	"time"
)

//type StatusCollector struct {
//	//control *Control
//	SentPackets     uint64
//	ReceivedPackets uint64
//	LostPackets     uint64
//	DroppedPackets  uint64
//	SmoothedRTT     uint64
//	LostRate        float64
//	LastUpdate      time.Time
//}

type NetStatusControl struct {
	IsShowStatus bool
}

type NetStatusTracer struct {
	SentPackets     uint64
	ReceivedPackets uint64
	LostPackets     uint64
	DroppedPackets  uint64
	SmoothedRTT     uint64
	//丢包率
	LostRate float64
	//丢弃率
	DroppedRate float64
	//带宽预估 bandwidth estimate
	BWEstimate float64
	LastUpdate time.Time
	control    *NetStatusControl
}

type NetStatusRecord struct {
	tracer *NetStatusTracer
}

func NewNetStatusTracer(ctrl *NetStatusControl) *NetStatusTracer {
	return &NetStatusTracer{
		control: ctrl,
	}
}

func (t *NetStatusTracer) SupportsSchemas(schema string) bool {
	slog.Info("SupportsSchemas", slog.String("schema", schema))
	return true
}

func (t *NetStatusTracer) AddProducer() qlogwriter.Recorder {
	r := &NetStatusRecord{
		tracer: t,
	}
	return r
}

func (t *NetStatusRecord) RecordEvent(evt qlogwriter.Event) {
	// 通过类型断言识别具体的 event 结构体
	switch e := evt.(type) {
	case qlog.PacketLost: // 1. 捕捉丢包事件
		atomic.AddUint64(&t.tracer.LostPackets, 1)
		//slog.Debug("[Tracer] 丢包通知:", slog.Any("PacketNumber", e.Header.PacketNumber),
		//	slog.Any("Trigger", e.Trigger))
		break
	case qlog.PacketSent:
		//slog.Debug("[Tracer] sent packet:", slog.Any("PacketNumber", e.Header.PacketNumber),
		//	slog.Any("Trigger", e.Trigger), slog.Any("datagramId", e.DatagramID),
		//	slog.Any("target", e.Header.DestConnectionID), slog.Any("source", e.Header.SrcConnectionID))
		atomic.AddUint64(&t.tracer.SentPackets, 1)
		break
	case qlog.PacketReceived:
		//slog.Debug("[Tracer] receive packet:", slog.Any("PacketNumber", e.Header.PacketNumber),
		//	slog.Any("Trigger", e.Trigger), slog.Any("datagramId", e.DatagramID),
		//	slog.Any("target", e.Header.DestConnectionID), slog.Any("source", e.Header.SrcConnectionID))
		atomic.AddUint64(&t.tracer.ReceivedPackets, 1)
		break
	case qlog.PacketDropped:
		atomic.AddUint64(&t.tracer.DroppedPackets, 1)
		break
	case qlog.CongestionStateUpdated: //拥塞状态变更 (SlowStart / Recovery 等)
		/*
			状态名称 			常用枚举 						表示触发条件
			慢启动				SlowStart / CongestionSlowStart	初始/探测状态。连接刚建立或超时重传后进入。拥塞窗口（cwnd）呈指数级增长，以快速探知网络可用带宽。
			拥塞避免				CongestionAvoidance				稳态传输。当 cwnd 超过慢启动阈值（ssthresh）后进入。cwnd 改为线性增长（每个 RTT 增加约 1 个 MSS），以安全地试探带宽上限。
			恢复 / 快速恢复		Recovery / FastRecovery			轻微拥塞/丢包。检测到丢包（例如收到 3 个重复 ACK 或 QUIC 丢包检测触发），但未发生超时。此时会降低 cwnd 并重传丢失的包，等待丢失数据恢复。
			应用受限 / 挂起		ApplicationLimited / Idle		发送端无数据可发。不是因为网络堵塞，而是应用程序自身没有更多数据要发送，导致 cwnd 暂停增长。
			超时重传 (部分实现包含)	Loss / RTO (Loss Recovery)		严重拥塞。发生重传超时（RTO）。系统会将 ssthresh 大幅降低，并将 cwnd 标记归零/重置，重新退回 SlowStart 状态。
		*/
		slog.Info("[Tracer]监测到网络状态发生变化，当前拥塞状态:", slog.Any("State", e.State))
		break
	case qlog.MetricsUpdated: //捕捉 RTT 或拥塞窗口 (CWND) 更新 ，从目前侦测的数据来看，6秒触发一次
		now := time.Now()
		if now.Sub(t.tracer.LastUpdate) < time.Second {
			return
		} //每秒更新一次即可
		t.tracer.LastUpdate = now
		if e.SmoothedRTT != 0 {
			atomic.StoreUint64(&t.tracer.SmoothedRTT, uint64(e.SmoothedRTT.Milliseconds()))
		}
		srtt := e.SmoothedRTT
		if srtt < time.Millisecond {
			srtt = time.Millisecond
		}
		bandwidthEstimate := uint64(e.CongestionWindow) * uint64(time.Second/srtt) * 8 //bps
		t.tracer.BWEstimate = float64(bandwidthEstimate) / 1000                        //kbps
		t.tracer.LostRate = float64(t.tracer.LostPackets*100) / float64(t.tracer.SentPackets)
		t.tracer.DroppedRate = float64(t.tracer.DroppedPackets*100) / float64(t.tracer.ReceivedPackets)
		if t.tracer.control.IsShowStatus {
			slog.Info("[Tracer]网络监控数据", slog.Any("带宽", bandwidthEstimate), slog.Any("拥塞窗口", e.CongestionWindow),
				slog.Any("在途数据量", e.BytesInFlight), slog.Any("在途数据包数", e.PacketsInFlight),
				slog.Any("最新往返时间", e.LatestRTT), slog.Any("平滑往返时间", t.tracer.SmoothedRTT),
				slog.Any("发送", t.tracer.SentPackets), slog.Any("丢包数", t.tracer.LostPackets), slog.Any("丢包率", t.tracer.LostRate),
				slog.Any("接收", t.tracer.ReceivedPackets), slog.Any("丢弃数", t.tracer.DroppedPackets), slog.Any("丢弃率", t.tracer.DroppedRate))
		}
		//todo:我们只计算这个统计周期内的数据
		atomic.SwapUint64(&t.tracer.LostPackets, 0)
		atomic.SwapUint64(&t.tracer.SentPackets, 0)
		atomic.SwapUint64(&t.tracer.ReceivedPackets, 0)
		atomic.SwapUint64(&t.tracer.DroppedPackets, 0)
		break
	// 4. 捕捉连接关闭事件
	case qlog.ConnectionClosed:
		slog.Info("[Tracer]连接关闭:", slog.Any("Reason", e.Reason))
		break
	}
}

func (t *NetStatusRecord) Close() error {
	return nil
}
