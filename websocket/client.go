package websocket

import (
	"crypto/tls"
	"fmt"
	"github.com/DeleteElf/zero-net/framework"
	"github.com/DeleteElf/zero-net/framework/utils"
	"github.com/deleteelf/goframework/utils/jsonhelper"
	"github.com/gorilla/websocket"
	"log/slog"
	"time"
)

var DefaultHeartMessage string = "{\"action\":\"ping\",\"from\":\"host\"}"

type Client struct {
	Conn            *websocket.Conn
	heartTicker     *time.Ticker
	lastMessageTime time.Time
	lastHeartTime   time.Time
	HeartTimeout    time.Duration
	framework.CloseableObject
	//接收消息时，是否异步回调
	AsyncMessage   bool
	Connected      bool
	Reconnect      bool
	Reason         string
	OnMessage      func(msg string)
	OnConnected    func(msg string)
	OnDisconnected func(msg string)

	Address      string
	HeartMessage string
}

func NewClient() *Client {
	cli := &Client{
		HeartTimeout: time.Second * 50,
		AsyncMessage: true,
		Reason:       "",
		Reconnect:    true,
	}
	cli.IsClosed = false
	cli.SetOnCloseHandler(cli)
	return cli
}

func (c *Client) Connect(address, heartMessage string) error {
	c.Address = address
	c.HeartMessage = heartMessage
	websocket.DefaultDialer.HandshakeTimeout = 10 * time.Second
	websocket.DefaultDialer.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: true,
	}
	ws, _, err := websocket.DefaultDialer.Dial(address, nil)
	if err != nil {
		if c.Reconnect {
			go c.ReconnectWorking()
		}
		return err
	}
	c.Conn = ws
	c.lastMessageTime = time.Now()
	c.lastHeartTime = time.Now()
	c.Connected = true
	if c.OnConnected != nil {
		c.OnConnected(fmt.Sprintf("{\"local\":\"%s\",\"remote\":\"%s\"}",
			ws.LocalAddr().String(), ws.RemoteAddr().String()))
	}
	c.Conn.SetCloseHandler(func(code int, text string) error {
		c.Reason = fmt.Sprintf("{\"code\":%d,\"msg\":\"%s\"}", code, text)
		c.Disconnect()
		return nil
	})
	go func() {
		defer c.Disconnect()
		for {
			if c.Conn == nil {
				break
			}
			if !c.Connected {
				break
			}
			if c.IsClosed {
				break
			}
			_, msg, err := c.Conn.ReadMessage()
			if err != nil {
				c.Reason = fmt.Sprintf("{\"code\":%d,\"msg\":\"%s\"}", 2, err.Error())
				break
			}
			c.lastMessageTime = time.Now()
			slog.Debug("接收消息", slog.Any("body", msg))
			result, err := utils.GetJsonObject(msg)
			if err != nil {
				c.Reason = fmt.Sprintf("{\"code\":%d,\"msg\":\"%s\"}", 3, err.Error())
				break
			}
			if result["Action"] == "pong" {
				continue
			}
			if c.OnMessage != nil {
				if c.AsyncMessage {
					go func() {
						c.OnMessage(string(msg))
					}()
				} else {
					c.OnMessage(string(msg))
				}
			}
		}
	}()
	c.Heart(c.HeartMessage)
	return nil
}

func (c *Client) Heart(heartMessage string) {
	if len(heartMessage) == 0 {
		return
	}
	if c.HeartMessage != heartMessage {
		c.HeartMessage = heartMessage
	}
	tickerDuration := c.HeartTimeout
	expireDuration := c.HeartTimeout + 10*time.Second
	c.heartTicker = time.NewTicker(time.Second) //每秒检查一次
	defer c.Disconnect()
	for range c.heartTicker.C {
		if c.Conn == nil {
			break
		}
		if !c.Connected {
			break
		}
		if c.IsClosed {
			break
		}
		if c.lastMessageTime.Add(expireDuration).Compare(time.Now()) < 0 {
			slog.Warn("pong time out")
			c.Reason = "{\"code\":1,\"msg\":\"ping time out\"}"
			break
		}
		if c.lastHeartTime.Add(tickerDuration).Compare(time.Now()) < 0 {
			c.lastHeartTime = time.Now()
			_ = c.Send(heartMessage)
		}
	}
	slog.Warn("stop ping heart")
}

func (c *Client) OnClosing() bool {
	c.Reconnect = false
	c.Reason = "主动断开连接"
	c.Disconnect()
	return true
}

func (c *Client) OnClosed() error {
	slog.Debug("websocket已经销毁...")
	return nil
}

func (c *Client) SendJson(v any) error {
	jsonString, err := utils.ToJsonString(v)
	if err != nil {
		return err
	}
	return c.Send(jsonString)
}

func (c *Client) Send(msg string) error {
	if c.Conn != nil && !c.IsClosed {
		slog.Debug("发送消息", slog.String("body", msg))
		return c.Conn.WriteMessage(websocket.TextMessage, []byte(msg))
	}
	return nil
}

func (c *Client) GetLocalAddr() string {
	return c.Conn.LocalAddr().String()
}
func (c *Client) GetRemoteAddr() string {
	return c.Conn.RemoteAddr().String()
}
func (c *Client) Disconnect() {
	if c.Connected {
		slog.Debug("正在断开websocket连接...")
		if c.OnDisconnected != nil {
			c.OnDisconnected(c.Reason)
			reason, err := jsonhelper.GetJsonObject([]byte(c.Reason))
			if err != nil {
				if reason["code"] != nil && reason["msg"] != nil {
					//todo：本来是考虑如果合理断开，就不能再连了,但是风险太高，明确需要是"被管理员强制下线"，才不再重连，除非重启
					if reason["code"].(int) == 0 && reason["msg"].(string) == "disconnect by admin" {
						c.Reconnect = false
					}
				}
			}
			c.Reason = ""
		}
		if c.heartTicker != nil {
			c.heartTicker.Stop()
			c.heartTicker = nil
		}
		if c.Conn != nil {
			_ = c.Conn.Close()
			c.Conn = nil
		}
		c.Connected = false
		slog.Debug("websocket已经断开！")
		if c.Reconnect {
			go c.ReconnectWorking()
		}
	}
}

func (c *Client) ReconnectWorking() {
	//for {
	time.Sleep(1 * time.Second)
	if c.Reconnect == true && c.Connected == false {
		err := c.Connect(c.Address, c.HeartMessage)
		if err != nil {
			slog.Error("重新连接发生错误", slog.Any("err", err))
		}
	}
	//	if c.Reconnect == false || c.Connected {
	//		break
	//	}
	//}
}
