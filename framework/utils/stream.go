package utils

import (
	"context"
	"encoding/binary"
	"errors"
	"github.com/quic-go/quic-go"
	"io"
	"log/slog"
	"time"
)

type StreamResult struct {
	data []byte
	err  error
}

// HeaderType 数据包头的类型
type HeaderType int

const (
	UShort HeaderType = 2
	UInt              = 4
	ULong             = 8
)

// ReadStreamByHeaderUShort 从数据流中读取数据包，数据包长度类型为uint16，且未接收完流前不会返回
func ReadStreamByHeaderUShort(stream *quic.Stream) ([]byte, error) {
	return ReadStreamByHeaderType(stream, UShort)
}

// ReadStreamByHeaderType 从数据流中读取数据包，数据包的长度又类型提供简单的支持，且未接收完流前不会返回
func ReadStreamByHeaderType(stream *quic.Stream, headerType HeaderType) ([]byte, error) {
	//packetHeader, err := ReadStreamWithTimeout(stream, uint64(headerType), time.Second) //todo:如果一直在退出时，卡住，则允许在读取头时超时,一直超时也不对，需求更好的方式
	packetHeader, err := ReadStreamFull(stream, uint64(headerType))
	if err != nil {
		return packetHeader, err
	}
	var length uint64
	if len(packetHeader) > 0 {
		switch headerType {
		case UInt:
			length = uint64(binary.BigEndian.Uint32(packetHeader))
			break
		case ULong:
			length = binary.BigEndian.Uint64(packetHeader)
			break
		default:
			length = uint64(binary.BigEndian.Uint16(packetHeader))
			break
		}
	}
	//slog.Debug("从流中读取到数据", slog.Int("长度", length))
	return ReadStreamFull(stream, length)
}

// ReadStreamWithTimeout 从数据流中读取数据包，在接收完流的指定长度前不会返回
func ReadStreamWithTimeout(stream *quic.Stream, length uint64, timeout time.Duration) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ch := make(chan StreamResult)
	go func() {
		if length > 0 {
			buf := make([]byte, length) //根据数据包长度建立缓存
			//count, err := io.ReadFull(stream, buf) //读取一个完整的数据
			err := stream.SetReadDeadline(time.Now().Add(timeout))
			if err != nil {
				return
			}
			stream.SetReliableBoundary()
			count, err := stream.Read(buf)
			if err != nil {
				slog.Error("从流中读取数据发生错误", slog.String("err", err.Error()))
				return
			}
			ch <- StreamResult{buf[:count], err}
		}
	}()
	select {
	case result := <-ch:
		return result.data, result.err
	case <-ctx.Done():
		return []byte{}, ctx.Err() // 返回 context.DeadlineExceeded
	}
}

// ReadStream 从数据流中读取数据包，在接收完流的指定长度前不会返回
func ReadStream(stream *quic.Stream, length uint64) ([]byte, error) {
	if length > 0 {
		buf := make([]byte, length) //根据数据包长度建立缓存
		//count, err := io.ReadFull(stream, buf) //读取一个完整的数据
		count, err := stream.Read(buf)
		if err != nil {
			return []byte{}, err
		}
		if count == 0 { //读取到0长度的数据包，我们认为是断开连接了
			return []byte{}, errors.New("stream read fail")
		}
		return buf[:count], nil
	}
	return []byte{}, nil
}

// ReadStreamFull 从数据流中读取数据包，在接收完流的指定长度前不会返回
func ReadStreamFull(stream *quic.Stream, length uint64) ([]byte, error) {
	if length > 0 {
		buf := make([]byte, length)            //根据数据包长度建立缓存
		count, err := io.ReadFull(stream, buf) //读取一个完整的数据
		if err != nil {
			return []byte{}, err
		}
		if count == 0 { //读取到0长度的数据包，我们认为是断开连接了
			return []byte{}, errors.New("stream read fail")
		}
		return buf[:count], nil
	}
	return []byte{}, nil
}

// WriteStreamByHeaderUShort 将数据写入数据流，同时携带数据包长度作为报头，数据包长度类型为uint16
func WriteStreamByHeaderUShort(stream *quic.Stream, data []byte) error {
	return WriteStreamByHeaderType(stream, data, UShort)
}

// WriteStreamByHeaderType 将数据写入数据流，同时携带数据包长度作为报头，数据包长度类型根据输入headerType进行指定
func WriteStreamByHeaderType(stream *quic.Stream, data []byte, headerType HeaderType) error {
	if len(data) == 0 {
		return nil
	}
	packetHeader := make([]byte, headerType)
	switch headerType {
	case UInt:
		binary.BigEndian.PutUint32(packetHeader, uint32(len(data)))
		return WriteStream(stream, packetHeader, data)
	case ULong:
		binary.BigEndian.PutUint64(packetHeader, uint64(len(data)))
		return WriteStream(stream, packetHeader, data)
	default:
		binary.BigEndian.PutUint16(packetHeader, uint16(len(data)))
		return WriteStream(stream, packetHeader, data)
	}
}

// WriteStream 将数据包写入数据流，数据包包含数据头和载体，数据头允许为空
func WriteStream(stream *quic.Stream, header, payload []byte) error {
	if len(header) > 0 {
		count, err := stream.Write(append(header, payload...)) //保证数据的连续性
		if err != nil {
			return err
		}
		if count == 0 {
			return errors.New("stream header write fail")
		}
	} else {
		count, err := stream.Write(payload)
		if err != nil {
			return err
		}
		if count == 0 {
			return errors.New("stream payload write fail")
		}
	}
	return nil
}
