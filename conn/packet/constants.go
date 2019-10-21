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

package packet

import "errors"

// 定义网络包的类型 Packet
// Type represents the network packet's type such as: handshake and so on.
type Type byte

const (
	_ Type = iota
	// Handshake represents a handshake: request(client) <====> handshake response(server)
	Handshake = 0x01 //客户端到服务端的捂手请求已经服务端到客户端的捂手相应

	// HandshakeAck represents a handshake ack from client to server
	HandshakeAck = 0x02 //客户端到服务端的握手ack

	// Heartbeat represents a heartbeat
	Heartbeat = 0x03 //心跳包

	// Data represents a common data packet
	Data = 0x04 //数据包

	// Kick represents a kick off packet
	Kick = 0x05 // disconnect message from server //服务端主动断开连接的通知
)

// ErrWrongPomeloPacketType represents a wrong packet type.
var ErrWrongPomeloPacketType = errors.New("wrong packet type")

//关于握手流程：1: c->s Handshake 2:s->Handshake 3: c->s HandshakeAck
//握手流程主要提供一个机会，让客户端和服务器在连接建立后，进行一些初始化的数据交换
//当底层连接建立后，客户端向服务器发起握手请求，并附带必要的数据。服务器检验握手数据后，返回握手响应。
//如果 握手成功，客户端向服务器发送一个握手ack，握手阶段至此成功结束
