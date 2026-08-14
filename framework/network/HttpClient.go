package network

import (
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net/http"
)

func HttpRequest(url, method, token string, body io.Reader) ([]byte, error) {
	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	httpClient := &http.Client{Transport: tr}
	request, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, fmt.Errorf("生成http出错: %w", err)
	}
	if len(token) > 0 {
		request.Header.Set("Authorization", token)
	}
	slog.Debug(url, slog.Any("body", body))
	resp, err := httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("请求http出错: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("请求http失败,错误码: %s", resp.Status)
	}
	return io.ReadAll(resp.Body)
}
