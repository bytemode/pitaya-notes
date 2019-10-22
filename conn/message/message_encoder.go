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

package message

import (
	"encoding/binary"

	"github.com/topfreegames/pitaya/util/compression"
)

// Encoder interface
type Encoder interface {
	IsCompressionEnabled() bool
	Encode(message *Message) ([]byte, error)
}

// MessagesEncoder implements MessageEncoder interface
type MessagesEncoder struct {
	DataCompression bool //gzip压缩
}

// NewMessagesEncoder returns a new message encoder
func NewMessagesEncoder(dataCompression bool) *MessagesEncoder {
	me := &MessagesEncoder{dataCompression}
	return me
}

// IsCompressionEnabled returns wether the compression is enabled or not
func (me *MessagesEncoder) IsCompressionEnabled() bool {
	return me.DataCompression
}

//message编码方式
//消息头
//----------flag(1byte) --------------------------------------------------->message id(0~5byte)------>route(0~256byte)
//preserved(3bit) + 1bit(gzip) + message type(3bit) + route zip flag(1bit)   变长编码                 2byte的route code或1bite长度字符串
//数据部分
//根据是否启用gzip压缩在flag的第5个bit写标志,之后再消息头之后添加消息内容

// Encode marshals message to binary format. Different message types is corresponding to
// different message header, message types is identified by 2-4 bit of flag field. The
// relationship between message types and message header is presented as follows:
// ------------------------------------------
// |   type   |  flag  |       other        |
// |----------|--------|--------------------|
// | request  |----000-|<message id>|<route>|
// | notify   |----001-|<route>             |
// | response |----010-|<message id>        |
// | push     |----011-|<route>             |
// ------------------------------------------
// The figure above indicates that the bit does not affect the type of message.
// See ref: https://github.com/topfreegames/pitaya/blob/master/docs/communication_protocol.md
func (me *MessagesEncoder) Encode(message *Message) ([]byte, error) {
	if invalidType(message.Type) {
		return nil, ErrWrongMessageType
	}

	//flag 1byte包含： 4bit保留 + 3bit(2 3 4)message type字段 + 1bit route标识
	buf := make([]byte, 0)
	flag := byte(message.Type) << 1 //左移1bit 四中类型的type字段放在2 3 4bit

	code, compressed := routes[message.Route] //使用路由Route获取路由字典编码
	if compressed {
		flag |= msgRouteCompressMask //0x01 写入路由压缩的标志 flag第一位写入路由压缩标识
	}

	if message.Err {
		flag |= errorMask
	}

	buf = append(buf, flag) //标志位(flag)占用messagede第一个字节

	//request和response需要携带message id
	if message.Type == Request || message.Type == Response {
		n := message.ID
		// variant length encode 可变长编码
		for {
			b := byte(n % 128)
			n >>= 7
			if n != 0 {
				buf = append(buf, b+128)
			} else {
				buf = append(buf, b)
				break
			}
		}
	}

	//route信息
	if routable(message.Type) {
		if compressed {
			//如果启用路由压缩则使用2byte存储 route code
			buf = append(buf, byte((code>>8)&0xFF))
			buf = append(buf, byte(code&0xFF))
		} else {
			//如果不启用路由压缩则使用带1个字节长度的字串存储route 256B
			buf = append(buf, byte(len(message.Route)))
			buf = append(buf, []byte(message.Route)...)
		}
	}

	//对消息体进行二进制压缩 gzip
	if me.DataCompression {
		d, err := compression.DeflateData(message.Data)
		if err != nil {
			return nil, err
		}

		if len(d) < len(message.Data) {
			//压缩成功则将压缩后的值覆盖原有message.Data
			message.Data = d
			buf[0] |= gzipMask //0x10  0001 0000  使用flag的第5位来存储gzip压缩标志
		}
	}

	buf = append(buf, message.Data...) //消息头和消息体合并在一起
	return buf, nil
}

// Decode decodes the message
func (me *MessagesEncoder) Decode(data []byte) (*Message, error) {
	return Decode(data)
}

// Decode unmarshal the bytes slice to a message
// See ref: https://github.com/topfreegames/pitaya/blob/master/docs/communication_protocol.md
func Decode(data []byte) (*Message, error) {
	if len(data) < msgHeadLength {
		return nil, ErrInvalidMessage
	}
	m := New()
	flag := data[0] //取出第一个字节作为flag
	offset := 1
	m.Type = Type((flag >> 1) & msgTypeMask) //取出flag 2 3 4 bit作为消息的 type

	if invalidType(m.Type) {
		return nil, ErrWrongMessageType
	}

	//读取出message id base 128 variant
	if m.Type == Request || m.Type == Response {
		id := uint(0)
		// little end byte order
		// WARNING: must can be stored in 64 bits integer
		// variant length encode
		for i := offset; i < len(data); i++ {
			b := data[i]
			id += uint(b&0x7F) << uint(7*(i-offset))
			if b < 128 {
				offset = i + 1
				break
			}
		}
		m.ID = id
	}

	m.Err = flag&errorMask == errorMask

	if routable(m.Type) {
		if flag&msgRouteCompressMask == 1 {
			//读取route code (2byte) 根据code得到route
			m.compressed = true
			code := binary.BigEndian.Uint16(data[offset:(offset + 2)])
			route, ok := codes[code]
			if !ok {
				return nil, ErrRouteInfoNotFound
			}
			m.Route = route
			offset += 2
		} else {
			//读取一个字节作为字符串长度然后根据读取route字符串
			m.compressed = false
			rl := data[offset]
			offset++
			m.Route = string(data[offset:(offset + int(rl))])
			offset += int(rl)
		}
	}

	//剩余部分为数据部分 根据flag的第5bit来进行解压
	m.Data = data[offset:]
	var err error
	if flag&gzipMask == gzipMask {
		m.Data, err = compression.InflateData(m.Data)
		if err != nil {
			return nil, err
		}
	}
	return m, nil
}
