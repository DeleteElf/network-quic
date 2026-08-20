package network

import (
	"fmt"
	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
	"log/slog"
	"sync/atomic"
	"time"
)

type NetStatusControl struct {
	IsShowStatus bool
}

type NetStatusTracer struct {
	SentPackets     uint64
	ReceivedPackets uint64
	LostPackets     uint64
	DroppedPackets  uint64
	SmoothedRTT     uint64
	LastRTT         uint64
	//丢包率
	LostRate float64
	//丢弃率
	DroppedRate float64
	//带宽预估 bandwidth estimate
	BWEstimate          float64
	LastCwnd            uint64
	LastUpdate          time.Time
	LastCongestionState string
	control             *NetStatusControl
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
	case qlog.PacketSent:
		atomic.AddUint64(&t.tracer.SentPackets, 1)
	case qlog.PacketReceived:
		atomic.AddUint64(&t.tracer.ReceivedPackets, 1)
	case qlog.PacketDropped:
		atomic.AddUint64(&t.tracer.DroppedPackets, 1)
	case qlog.CongestionStateUpdated: //拥塞状态变更 (SlowStart / Recovery 等)

		/*
			状态名称 			常用枚举 						表示触发条件
			慢启动				SlowStart / CongestionSlowStart	初始/探测状态。连接刚建立或超时重传后进入。拥塞窗口（cwnd）呈指数级增长，以快速探知网络可用带宽。
			拥塞避免				CongestionAvoidance				稳态传输。当 cwnd 超过慢启动阈值（ssthresh）后进入。cwnd 改为线性增长（每个 RTT 增加约 1 个 MSS），以安全地试探带宽上限。
			恢复 / 快速恢复		Recovery / FastRecovery			轻微拥塞/丢包。检测到丢包（例如收到 3 个重复 ACK 或 QUIC 丢包检测触发），但未发生超时。此时会降低 cwnd 并重传丢失的包，等待丢失数据恢复。
			应用受限 / 挂起		ApplicationLimited / Idle		发送端无数据可发。不是因为网络堵塞，而是应用程序自身没有更多数据要发送，导致 cwnd 暂停增长。
			超时重传 (部分实现包含)	Loss / RTO (Loss Recovery)		严重拥塞。发生重传超时（RTO）。系统会将 ssthresh 大幅降低，并将 cwnd 标记归零/重置，重新退回 SlowStart 状态。
		*/
		if t.tracer.control.IsShowStatus {
			switch e.State {
			case qlog.CongestionStateSlowStart:
				slog.Info("网络监控，当前状态:探测可用带宽！")
			case qlog.CongestionStateRecovery:
				slog.Info("网络监控，当前状态:轻微拥塞！")
			default:
				if t.tracer.LastCongestionState == string(qlog.CongestionStateRecovery) {
					slog.Info("网络监控，当前状态：从拥塞中恢复！")
				}
			}
		}
		t.tracer.LastCongestionState = string(e.State)
	case qlog.MetricsUpdated: //捕捉 RTT 或拥塞窗口 (CWND) 更新 ，从目前侦测的数据来看，6秒触发一次
		// 1. 持续更新本地缓存（CWND 和 RTT 增量更新机制）
		if e.CongestionWindow > 0 {
			atomic.StoreUint64(&t.tracer.LastCwnd, uint64(e.CongestionWindow))
		}
		if e.SmoothedRTT.Milliseconds() > 0 {
			atomic.StoreUint64(&t.tracer.SmoothedRTT, uint64(e.SmoothedRTT.Milliseconds()))
		}
		if e.LatestRTT.Milliseconds() > 0 {
			atomic.StoreUint64(&t.tracer.LastRTT, uint64(e.LatestRTT.Milliseconds()))
		}
		now := time.Now()
		if now.Sub(t.tracer.LastUpdate) < time.Second {
			return
		} //每秒更新一次即可
		t.tracer.LastUpdate = now
		cwnd := atomic.LoadUint64(&t.tracer.LastCwnd)
		rttMs := atomic.LoadUint64(&t.tracer.SmoothedRTT)
		if rttMs > 0 && cwnd > 0 {
			srttSec := float64(rttMs) / 1000.0
			bandwidthEstimate := float64(cwnd) * 8.0 / srttSec      //bps
			t.tracer.BWEstimate = float64(bandwidthEstimate) / 1000 //kbps
		} else {
			t.tracer.BWEstimate = 0
		}

		sent := atomic.SwapUint64(&t.tracer.SentPackets, 0)
		lost := atomic.SwapUint64(&t.tracer.LostPackets, 0)
		recv := atomic.SwapUint64(&t.tracer.ReceivedPackets, 0)
		drop := atomic.SwapUint64(&t.tracer.DroppedPackets, 0)

		if sent == 0 {
			t.tracer.LostRate = 0
		} else {
			t.tracer.LostRate = float64(lost*100) / float64(sent)
		}
		if recv == 0 {
			t.tracer.DroppedRate = 0
		} else {
			t.tracer.DroppedRate = float64(drop*100) / float64(recv)
		}
		if t.tracer.control.IsShowStatus {
			slog.Info("网络监控数据", slog.Any("当前流量(byte/RTT)", cwnd),
				slog.Any("在途数据量(byte)", e.BytesInFlight), slog.Any("在途数据包数", e.PacketsInFlight),
				slog.Any("最新往返时间(ms)", t.tracer.LastRTT), slog.Any("平滑往返时间(ms)", rttMs),
				slog.Any("发送数", sent), slog.Any("丢包数", lost), slog.Any("丢包率(%)", fmt.Sprintf("%.2f", t.tracer.LostRate)),
				slog.Any("接收数", recv), slog.Any("丢弃数", drop), slog.Any("丢弃率(%)", fmt.Sprintf("%.2f", t.tracer.DroppedRate)),
				slog.Any("预估带宽(kbps)", fmt.Sprintf("%.3f", t.tracer.BWEstimate)))
		}
	// 4. 捕捉连接关闭事件
	case qlog.ConnectionClosed:
		slog.Info("[Tracer]连接关闭:", slog.Any("Reason", e.Reason))
	}
}

func (t *NetStatusRecord) Close() error {
	return nil
}
