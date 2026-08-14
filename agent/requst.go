package agent

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/DeleteElf/zero-net/framework/network"
	"io"
	"log/slog"
	"net"
	"net/http"
)

type Requst struct {
	MgrAddr string `json:"mgr_addr"`  // 管理平台的地址
	Token   string `json:"token"`     // 管理平台的token
	DevId   string `json:"dev_id"`    // 管理平台的dev_id
	ProxyId string `json:"proxy_id"`  // 管理平台的proxy_id
	Proxy   bool   `json:"proxy"`     // 是否代理
	CliId   string `json:"client_id"` // 客户端ID
	NetType string // 网络类型
	SvcType string // 服务地址
	SvcAddr string `json:"service"`  // 目标地址
	LoAddr  string `json:"lo_addr"`  // 本地监听地址，此时需要设置 up_buf down_buf
	UpBuf   int    `json:"up_buf"`   // TCP的上行缓冲区大小，默认64KB，此大小会影响吞吐、延迟
	DownBuf int    `json:"down_buf"` // TCP的下行缓冲区大小，默认64KB，此大小会影响吞吐、延迟
}

func GetProxy(req *Requst) (*ProxyInfo, error) {
	var proxy *ProxyInfo = nil
	if req.Proxy {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		client := &http.Client{Transport: tr}

		// 创建Request对象
		urlGet := fmt.Sprintf("%s/proxy?device_id=%s&proxy_id=%s", req.MgrAddr, req.DevId, req.ProxyId)
		slog.Debug("正在请求管理中心：", slog.String("url", urlGet))
		reqGet, err := http.NewRequest("GET", urlGet, nil)
		if err != nil {
			return nil, fmt.Errorf("create request err: %v", err)
		}
		reqGet.Header.Set("Authorization", req.Token)
		var authResult ActionProxy
		// 发送请求
		resp, err := client.Do(reqGet)
		if err != nil {
			//if i == maxTry-1 {
			return nil, fmt.Errorf("requst url err: %v", err)
			//}
			//time.Sleep(500 * time.Millisecond)
			//continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			//if i == maxTry-1 {
			return nil, fmt.Errorf("respone status: %s", resp.Status)
			//}
			//time.Sleep(500 * time.Millisecond)
			//continue
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			//if i == maxTry-1 {
			return nil, fmt.Errorf("read err: %v", err)
			//}
			//time.Sleep(500 * time.Millisecond)
			//continue
		}

		slog.Info(string(body))
		if err := json.Unmarshal(body, &authResult); err != nil {
			return nil, fmt.Errorf("mgr resp json unmarshal err: %v", err)
		}
		if !authResult.IsSuccess() {
			//if i == maxTry-1 {
			return nil, fmt.Errorf("mgr resp not success")
			//}
			//time.Sleep(500 * time.Millisecond)
			//continue
		}
		proxy = &authResult.Data

		if len(proxy.ProxyAddr) == 0 {
			proxy.ProxyAddr = proxy.ProxyExternalIp + ":" + proxy.ProxyExternalPort
		}
		slog.Debug("获取到的代理地址：", slog.String("address", proxy.ProxyAddr), slog.Int("idx", proxy.Idx))
		if _, err := net.ResolveUDPAddr(network.STREAM_NETWORK_UDP, proxy.ProxyAddr); err != nil {
			return nil, fmt.Errorf("mgr resp proxy addr %s err: %v", proxy.ProxyAddr, err)
		}
	}
	return proxy, nil
}
