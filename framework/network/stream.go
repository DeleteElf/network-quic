package network

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/DeleteElf/zero-net/framework/utils"
	"github.com/quic-go/quic-go"
	"log/slog"
	"time"
)

type StreamType int

const (
	Control StreamType = iota
	Audio
	Video
	Message
)

// signSalt, 使用 openssl rand -hex 32 生成，每个版本不一样，定期删除过旧的版本
const (
	STREAM_SIGN_VER_NEWEST  = "1"
	STREAM_SIGN_SALT_NEWEST = "fcfd186998e061eb3e297be7da42fe017fc8269f2963e2977502c676bae1068c"

	STREAM_NETWORK_TCP = "tcp"
	STREAM_NETWORK_UDP = "udp4"
	DefaultBufferSize  = 7 * 1024 * 1024

	STREAM_TYPE_NORMAL = 0 //普通数据流，我们走go的标准stream
	STREAM_TYPE_FEC    = 1 //基于fec支持的数据流，我们使用普通的udp传输，并实现fec算法
)

type StreamInfo struct {
	Id      string `json:"id"`
	Version string `json:"v"` // 客户端的版本号
	Type    int    `json:"type"`
	Ts      int64  `json:"t"` // 用法见 ValidateStreamInfo
	Sign    string `json:"s"` // 客户端的签名，用于服务端校验客户端的合法性

	DataShards   int `json:"d"`
	ParityShards int `json:"p"`

	Index int `json:"i"`
	Count int `json:"c"`
}

var ss = map[string]string{
	STREAM_SIGN_VER_NEWEST: STREAM_SIGN_SALT_NEWEST,
}

func ValidateStreamInfo(info *StreamInfo) error {
	diffTs := time.Now().Unix() - info.Ts
	if diffTs > 300 || diffTs < -300 {
		return fmt.Errorf("invalid ts")
	}
	salt, ok := ss[info.Version]
	if !ok {
		return fmt.Errorf("invalid ver")
	}
	sign := utils.EncryptBytes([]byte(fmt.Sprintf("%s_%d", salt, info.Ts)))
	if sign != info.Sign {
		return fmt.Errorf("invalid sign")
	}
	return nil
}

func CloseStream(stream *quic.Stream) error {
	stream.CancelRead(0)
	err := stream.Close()
	stream.CancelWrite(0)
	return err
}

func CreateStream(quicConn *quic.Conn, info StreamInfo) (*quic.Stream, error) {
	info.Version = STREAM_SIGN_VER_NEWEST
	info.Sign = utils.EncryptBytes([]byte(fmt.Sprintf("%s_%d", STREAM_SIGN_SALT_NEWEST, info.Ts)))
	jsonInfo, err := json.Marshal(info)
	if err != nil {
		return nil, err
	}
	stream, err := quicConn.OpenStreamSync(context.TODO())
	if err != nil {
		return nil, err
	}
	if err = SendStreamData(stream, jsonInfo); err != nil {
		err = stream.Close()
		if err != nil {
			return nil, err
		}
		return nil, err
	}
	return stream, nil
}

func ReadStreamInfo(stream *quic.Stream) (*StreamInfo, error) {
	streamId := stream.StreamID()
	infoLen, err := ReadStreamLen(stream)
	if err != nil {
		return nil, err
	}
	if infoLen < 0 || infoLen > 1024 {
		slog.Warn("quic stream recv len invalid", slog.Any("streamId", streamId), slog.Int64("infoLen", infoLen))
		return nil, fmt.Errorf("invalid stream info")
	}
	jsonInfo := make([]byte, infoLen)
	if err := ReadStream(stream, jsonInfo); err != nil {
		return nil, err
	}
	var info StreamInfo
	err = json.Unmarshal(jsonInfo, &info)
	return &info, err
}

func SendStreamData(stream *quic.Stream, buf []byte) error {
	infoLen := int64(len(buf))
	var head [binary.MaxVarintLen64]byte
	headLen := binary.PutVarint(head[:], infoLen)

	if _, err := stream.Write(head[:headLen]); err != nil {
		return err
	}
	if infoLen > 0 {
		if _, err := stream.Write(buf); err != nil {
			return err
		}
	}
	return nil
}

func ReadStreamLen(stream *quic.Stream) (int64, error) {
	headLen := 0
	var head [binary.MaxVarintLen64]byte
	for headLen < binary.MaxVarintLen64 {
		if _, err := stream.Read(head[headLen : headLen+1]); err != nil {
			return 0, err
		}
		if head[headLen] < 0x80 {
			headLen++
			break
		}
		headLen++
	}
	dataLen, _ := binary.Varint(head[:headLen])
	return dataLen, nil
}

func ReadStream(stream *quic.Stream, buf []byte) error {
	curLen := 0
	for {
		readLen, err := stream.Read(buf[curLen:])
		if err != nil {
			return err
		}
		curLen += readLen
		if curLen == len(buf) {
			return nil
		}
	}
}
