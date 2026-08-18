package framework

import (
	"io"
	"sync"
)

// OnCloseHandler 关闭的api接口
type OnCloseHandler interface {
	// OnClosing 关闭前需要处理的事务，如果要拒绝继续关闭，返回false
	OnClosing() bool
	// OnClosed 关闭后的事务，仅当完毕完成才会执行此业务
	OnClosed() error
}

// Closeable 可关闭对象
type Closeable interface {
	io.Closer
	SetOnCloseHandler(handler OnCloseHandler)
}

// CloseableObject 实现基础流程逻辑的关闭对象
type CloseableObject struct {
	IsClosed  bool
	closeLock sync.Mutex

	onCloseHandler OnCloseHandler
	Closeable
}

// SetOnCloseHandler 设置关闭时需要执行的句柄
func (c *CloseableObject) SetOnCloseHandler(handler OnCloseHandler) {
	c.onCloseHandler = handler
}

// Close 关闭流对象
func (c *CloseableObject) Close() error {
	c.closeLock.Lock()
	defer c.closeLock.Unlock()
	if !c.IsClosed { //防止多次执行
		//开始释放资源
		if c.onCloseHandler != nil {
			if c.onCloseHandler.OnClosing() {
				c.IsClosed = true
				return c.onCloseHandler.OnClosed()
			}
		}
	}
	return nil
}
