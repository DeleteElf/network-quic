package network

import "github.com/quic-go/quic-go/qlogwriter"

type StatusControl interface {
	Start()
	Stop()
	Run()
	OnCongestionStateChanged(qlogwriter.Recorder)
}

type NetStatusControl struct {
	ShowStatusLevel          StatusLevel
	OnCongestionStateChanged OnCongestionStateChangedCallback
	StatusControl
}

func (control *NetStatusControl) Start() {

}
func (control *NetStatusControl) Stop() {

}

// Run 运行网路监控调度，只要是处理丢包时，修改fec算法、控制分辨率、控制传输速度等
func (control *NetStatusControl) Run() {

}
