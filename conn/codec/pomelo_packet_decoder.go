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
	"bytes"

	"github.com/topfreegames/pitaya/conn/packet"
)

//二进制消息反序列换为Packet包

// PomeloPacketDecoder reads and decodes network data slice following pomelo's protocol
type PomeloPacketDecoder struct{}

// NewPomeloPacketDecoder returns a new decoder that used for decode network bytes slice.
func NewPomeloPacketDecoder() *PomeloPacketDecoder {
	return &PomeloPacketDecoder{}
}

func (c *PomeloPacketDecoder) forward(buf *bytes.Buffer) (int, packet.Type, error) {
	header := buf.Next(HeadLength) //读取缓冲区4byte的内容作为消息头
	typ := header[0]               //取出第一个字节作为消息的类型
	if typ < packet.Handshake || typ > packet.Kick {
		return 0, 0x00, packet.ErrWrongPomeloPacketType
	}
	size := bytesToInt(header[1:]) //取出2 3 4个字节做为消息体的长度

	// packet length limitation
	if size > MaxPacketSize {
		return 0, 0x00, ErrPacketSizeExcced
	}
	return size, packet.Type(typ), nil //返回header信息
}

// Decode decode the network bytes slice to packet.Packet(s)
func (c *PomeloPacketDecoder) Decode(data []byte) ([]*packet.Packet, error) {
	buf := bytes.NewBuffer(nil) //创建一个新的byte缓冲区
	buf.Write(data)

	var (
		packets []*packet.Packet
		err     error
	)
	// check length
	if buf.Len() < HeadLength { //如果进制的数据没有完整的消息头的长度则消息不完整
		return nil, nil
	}

	// first time
	size, typ, err := c.forward(buf) //读取消息头和消息类型
	if err != nil {
		return nil, err
	}

	for size <= buf.Len() { //二进制中的数据长度够消息体内容的长度 也就是可以满足本条消息的消息体长度
		p := &packet.Packet{Type: typ, Length: size, Data: buf.Next(size)} //读取消息体构建一个Packet
		packets = append(packets, p)                                       //Packet放入[]*packet.Packet

		// more packet
		if buf.Len() < HeadLength {
			break
		}

		size, typ, err = c.forward(buf) //接续读取下一个消息头的信息
		if err != nil {
			return nil, err
		}
	}

	return packets, nil
}

// Decode packet data length byte to int(Big end)
func bytesToInt(b []byte) int {
	result := 0
	for _, v := range b {
		result = result<<8 + int(v)
	}
	return result
}
