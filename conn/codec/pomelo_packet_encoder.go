// Copyright (c) nano Author and TFG Co. All Rights Reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package codec

import (
	"github.com/topfreegames/pitaya/conn/packet"
)

// PomeloPacketEncoder struct
type PomeloPacketEncoder struct {
}

// NewPomeloPacketEncoder ctor
func NewPomeloPacketEncoder() *PomeloPacketEncoder {
	return &PomeloPacketEncoder{}
}

//type(1byte) + length(3byte) + body(length bytes)
//package分为header和body两部分。header描述package包的类型和包的长度，body则是需要传输的数据内容。
//type - package类型，1个byte
// 	0x01: 客户端到服务器的握手请求以及服务器到客户端的握手响应
// 	0x02: 客户端到服务器的握手ack
// 	0x03: 心跳包
// 	0x04: 数据包
// 	0x05: 服务器主动断开连接通知
//length - body内容长度，3个byte的大端整数，因此最大的包长度为2^24个byte。
//body - 二进制的传输内容。

// Encode create a packet.Packet from  the raw bytes slice and then encode to network bytes slice
// Protocol refs: https://github.com/NetEase/pomelo/wiki/Communication-Protocol
//
// -<type>-|--------<length>--------|-<data>-
// --------|------------------------|--------
// 1 byte packet type, 3 bytes packet data length(big end), and data segment
func (e *PomeloPacketEncoder) Encode(typ packet.Type, data []byte) ([]byte, error) {
	if typ < packet.Handshake || typ > packet.Kick {
		return nil, packet.ErrWrongPomeloPacketType
	}

	if len(data) > MaxPacketSize {
		return nil, ErrPacketSizeExcced
	}

	p := &packet.Packet{Type: typ, Length: len(data)} //构建packet
	buf := make([]byte, p.Length+HeadLength)          //生成一个切片长度=消息头长度+消息体长度 header+body = 4 + len(body)
	buf[0] = byte(p.Type)                             //第一个字节存放消息类型

	copy(buf[1:HeadLength], intToBytes(p.Length)) //2~4 字节 存放消息长度
	copy(buf[HeadLength:], data)                  //4字节之后存放的内容是消息体

	return buf, nil
}

// 大端模式将int表位bytes
// Encode packet data length to bytes(Big end)
func intToBytes(n int) []byte {
	buf := make([]byte, 3)
	buf[0] = byte((n >> 16) & 0xFF)
	buf[1] = byte((n >> 8) & 0xFF)
	buf[2] = byte(n & 0xFF)
	return buf
}
