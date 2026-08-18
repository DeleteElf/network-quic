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
}

type NetStatusTracer struct {
	SentPackets     uint64
	ReceivedPackets uint64
	LostPackets     uint64
	DroppedPackets  uint64
	SmoothedRTT     uint64
	LostRate        float64
	LastUpdate      time.Time
	control         *NetStatusControl
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
		slog.Debug("[Tracer] 丢包通知:", slog.Any("PacketNumber", e.Header.PacketNumber),
			slog.Any("Trigger", e.Trigger))
		break
	case qlog.PacketSent:
		slog.Debug("[Tracer] sent packet:", slog.Any("PacketNumber", e.Header.PacketNumber),
			slog.Any("Trigger", e.Trigger), slog.Any("datagramId", e.DatagramID),
			slog.Any("target", e.Header.DestConnectionID), slog.Any("source", e.Header.SrcConnectionID))
		atomic.AddUint64(&t.tracer.SentPackets, 1)
		break
	case qlog.PacketReceived:
		slog.Debug("[Tracer] receive packet:", slog.Any("PacketNumber", e.Header.PacketNumber),
			slog.Any("Trigger", e.Trigger), slog.Any("datagramId", e.DatagramID),
			slog.Any("target", e.Header.DestConnectionID), slog.Any("source", e.Header.SrcConnectionID))
		atomic.AddUint64(&t.tracer.ReceivedPackets, 1)
		break
	case qlog.PacketDropped:
		atomic.AddUint64(&t.tracer.DroppedPackets, 1)
		break
	case qlog.CongestionStateUpdated: //拥塞状态变更 (SlowStart / Recovery 等)
		slog.Info("[Tracer] 当前拥塞状态:", slog.Any("State", e.State))
		break
	case qlog.MetricsUpdated: //捕捉 RTT 或拥塞窗口 (CWND) 更新
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
		bwEstimate := uint64(e.CongestionWindow) * uint64(time.Second) / uint64(srtt) * 8 / 1000
		t.tracer.LostRate = float64(t.tracer.LostPackets*100) / float64(t.tracer.SentPackets)
		slog.Debug("UpdatedMetrics", slog.Any("带宽", bwEstimate), slog.Any("拥塞窗口", e.CongestionWindow),
			slog.Any("在途数据量", e.BytesInFlight), slog.Any("在途数据包数", e.PacketsInFlight),
			slog.Any("最新往返时间", e.LatestRTT), slog.Any("平滑往返时间", t.tracer.SmoothedRTT),
			slog.Any("发送", t.tracer.SentPackets), slog.Any("丢包数", t.tracer.LostPackets),
			slog.Any("接收", t.tracer.ReceivedPackets), slog.Any("丢弃数", t.tracer.DroppedPackets),
			slog.Any("丢包率", t.tracer.LostRate))
		break
	// 4. 捕捉连接关闭事件
	case qlog.ConnectionClosed:
		slog.Info("[Tracer] 连接关闭:", slog.Any("Reason", e.Reason))
		break
	}
}

func (t *NetStatusRecord) Close() error {
	return nil
}
