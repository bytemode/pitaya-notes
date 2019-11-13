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

package agent

import (
	"context"
	gojson "encoding/json"
	e "errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/topfreegames/pitaya/conn/codec"
	"github.com/topfreegames/pitaya/conn/message"
	"github.com/topfreegames/pitaya/conn/packet"
	"github.com/topfreegames/pitaya/constants"
	"github.com/topfreegames/pitaya/errors"
	"github.com/topfreegames/pitaya/logger"
	"github.com/topfreegames/pitaya/metrics"
	"github.com/topfreegames/pitaya/protos"
	"github.com/topfreegames/pitaya/serialize"
	"github.com/topfreegames/pitaya/session"
	"github.com/topfreegames/pitaya/tracing"
	"github.com/topfreegames/pitaya/util"
	"github.com/topfreegames/pitaya/util/compression"

	opentracing "github.com/opentracing/opentracing-go"
)

//代理实体负责存储有关客户端连接的信息，它存储会话，编码器，串行器，状态，连接等。它用于与客户端通信以发送消息，并确保连接保持活动状态。

var (
	// hbd contains the heartbeat packet data
	hbd []byte
	// hrd contains the handshake response data
	hrd  []byte
	once sync.Once //Mutex和atomic组成的一个,Mutex保证并发atomic计数 并发下函数只会执行一次
)

const handlerType = "handler"

type (
	// Agent corresponds to a user and is used for storing raw Conn information
	Agent struct {
		Session *session.Session    // session
		chSend  chan pendingMessage // push message queue

		appDieChan      chan bool     // app die channel
		chDie           chan struct{} // wait for close
		chStopHeartbeat chan struct{} // stop heartbeats
		chStopWrite     chan struct{} // stop writing messages

		closeMutex         sync.Mutex          // Clsoe方法锁
		conn               net.Conn            // low-level conn fd  低级网络连接
		decoder            codec.PacketDecoder // binary decoder     packet解码
		encoder            codec.PacketEncoder // binary encoder     packet编码
		heartbeatTimeout   time.Duration       //心跳间隔
		lastAt             int64               // last heartbeat unix time stamp  //上次心跳时间
		messageEncoder     message.Encoder     //message编码
		messagesBufferSize int                 // size of the pending messages buffer 发送队列的消息长度
		metricsReporters   []metrics.Reporter
		serializer         serialize.Serializer // message serializer data部分的消息编码
		state              int32                // current agent state
	}

	pendingMessage struct {
		ctx     context.Context
		typ     message.Type // message type （push response）
		route   string       // message route (push) 消息路由信息
		mid     uint         // response message id (response) 消息id （response是存在）
		payload interface{}  // payload
		err     bool         // if its an error message
	}
)

// NewAgent create new agent instance
func NewAgent(
	conn net.Conn,
	packetDecoder codec.PacketDecoder,
	packetEncoder codec.PacketEncoder,
	serializer serialize.Serializer,
	heartbeatTime time.Duration,
	messagesBufferSize int,
	dieChan chan bool,
	messageEncoder message.Encoder,
	metricsReporters []metrics.Reporter,
) *Agent {
	// initialize heartbeat and handshake data on first user connection
	once.Do(func() {
		hbdEncode(heartbeatTime, packetEncoder, messageEncoder.IsCompressionEnabled(), serializer.GetName())
	})

	a := &Agent{
		appDieChan:         dieChan,
		chDie:              make(chan struct{}),
		chSend:             make(chan pendingMessage, messagesBufferSize), //发送消息的chan
		chStopHeartbeat:    make(chan struct{}),
		chStopWrite:        make(chan struct{}),
		messagesBufferSize: messagesBufferSize,
		conn:               conn,
		decoder:            packetDecoder,
		encoder:            packetEncoder,
		heartbeatTimeout:   heartbeatTime,     //心跳时间
		lastAt:             time.Now().Unix(), //上次通信时间
		serializer:         serializer,
		state:              constants.StatusStart,
		messageEncoder:     messageEncoder,
		metricsReporters:   metricsReporters,
	}

	// binding session   agent 和session相互关联
	s := session.New(a, true)
	metrics.ReportNumberOfConnectedClients(metricsReporters, session.SessionCount)
	a.Session = s
	return a
}

// 发送消息
func (a *Agent) send(m pendingMessage) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = errors.NewError(constants.ErrBrokenPipe, errors.ErrClientClosedRequest)
		}
	}()
	a.reportChannelSize()
	a.chSend <- m //发送消息都写入chSend chan
	return
}

// Push implementation for session.NetworkEntity interface
func (a *Agent) Push(route string, v interface{}) error {
	if a.GetStatus() == constants.StatusClosed {
		return errors.NewError(constants.ErrBrokenPipe, errors.ErrClientClosedRequest)
	}

	switch d := v.(type) {
	case []byte:
		logger.Log.Debugf("Type=Push, ID=%d, UID=%d, Route=%s, Data=%dbytes",
			a.Session.ID(), a.Session.UID(), route, len(d))
	default:
		logger.Log.Debugf("Type=Push, ID=%d, UID=%d, Route=%s, Data=%+v",
			a.Session.ID(), a.Session.UID(), route, v)
	}
	return a.send(pendingMessage{typ: message.Push, route: route, payload: v})
}

// ResponseMID implementation for session.NetworkEntity interface
// Respond message to session
func (a *Agent) ResponseMID(ctx context.Context, mid uint, v interface{}, isError ...bool) error {
	err := false
	if len(isError) > 0 {
		err = isError[0]
	}
	if a.GetStatus() == constants.StatusClosed {
		err := errors.NewError(constants.ErrBrokenPipe, errors.ErrClientClosedRequest)
		tracing.FinishSpan(ctx, err)
		metrics.ReportTimingFromCtx(ctx, a.metricsReporters, handlerType, err)
		return err
	}

	if mid <= 0 {
		err := constants.ErrSessionOnNotify
		tracing.FinishSpan(ctx, err)
		metrics.ReportTimingFromCtx(ctx, a.metricsReporters, handlerType, err)
		return err
	}

	switch d := v.(type) {
	case []byte:
		logger.Log.Debugf("Type=Response, ID=%d, UID=%d, MID=%d, Data=%dbytes",
			a.Session.ID(), a.Session.UID(), mid, len(d))
	default:
		logger.Log.Infof("Type=Response, ID=%d, UID=%d, MID=%d, Data=%+v",
			a.Session.ID(), a.Session.UID(), mid, v)
	}

	return a.send(pendingMessage{ctx: ctx, typ: message.Response, mid: mid, payload: v, err: err})
}

// Close closes the agent, cleans inner state and closes low-level connection.
// Any blocked Read or Write operations will be unblocked and return errors.
func (a *Agent) Close() error {
	a.closeMutex.Lock()
	defer a.closeMutex.Unlock()
	if a.GetStatus() == constants.StatusClosed { //读取状态判断是否已经关闭
		return constants.ErrCloseClosedSession
	}
	a.SetStatus(constants.StatusClosed) //设置关闭状态

	logger.Log.Debugf("Session closed, ID=%d, UID=%s, IP=%s",
		a.Session.ID(), a.Session.UID(), a.conn.RemoteAddr())

	// prevent closing closed channel
	select {
	case <-a.chDie:
		// expect
	default:
		close(a.chStopWrite)       //关闭写
		close(a.chStopHeartbeat)   //关闭心跳
		close(a.chDie)             //关闭等待关闭的通道
		onSessionClosed(a.Session) //通知session agent关闭
	}

	metrics.ReportNumberOfConnectedClients(a.metricsReporters, session.SessionCount)

	return a.conn.Close() //关闭网络连接
}

// RemoteAddr implementation for session.NetworkEntity interface
// returns the remote network address.
func (a *Agent) RemoteAddr() net.Addr {
	return a.conn.RemoteAddr()
}

// String, implementation for Stringer interface
func (a *Agent) String() string {
	return fmt.Sprintf("Remote=%s, LastTime=%d", a.conn.RemoteAddr().String(), atomic.LoadInt64(&a.lastAt))
}

// GetStatus gets the status
func (a *Agent) GetStatus() int32 {
	return atomic.LoadInt32(&a.state)
}

// Kick sends a kick packet to a client
func (a *Agent) Kick(ctx context.Context) error {
	// packet encode
	p, err := a.encoder.Encode(packet.Kick, nil)
	if err != nil {
		return err
	}
	_, err = a.conn.Write(p)
	return err
}

// SetLastAt sets the last at to now
func (a *Agent) SetLastAt() {
	atomic.StoreInt64(&a.lastAt, time.Now().Unix()) //记录的是上一次接收客户端消息的时间戳
}

// SetStatus sets the agent status
func (a *Agent) SetStatus(state int32) {
	atomic.StoreInt32(&a.state, state)
}

// Handle handles the messages from and to a client
func (a *Agent) Handle() {
	defer func() {
		a.Close()
		logger.Log.Debugf("Session handle goroutine exit, SessionID=%d, UID=%d", a.Session.ID(), a.Session.UID())
	}()

	go a.write()
	go a.heartbeat()

	//阻塞等待agent关闭 其实是等待Close之后此处才会收到消息
	select {
	case <-a.chDie: // agent closed signal 阻塞 如果close(a.chDie)调用则此处会读取到struct{}
		return
	}
}

// IPVersion returns the remote address ip version.
// net.TCPAddr and net.UDPAddr implementations of String()
// always construct result as <ip>:<port> on both
// ipv4 and ipv6. Also, to see if the ip is ipv6 they both
// check if there is a colon on the string.
// So checking if there are more than one colon here is safe.
func (a *Agent) IPVersion() string {
	version := constants.IPv4

	ipPort := a.RemoteAddr().String()
	if strings.Count(ipPort, ":") > 1 {
		version = constants.IPv6
	}

	return version
}

func (a *Agent) heartbeat() {
	ticker := time.NewTicker(a.heartbeatTimeout) //Ticker包含一个chan 间隔时间就向chan发送消息

	defer func() {
		ticker.Stop() //心跳检测停止
		a.Close()
	}()

	for {
		select {
		case <-ticker.C: //堵塞等待定时间chan
			deadline := time.Now().Add(-2 * a.heartbeatTimeout).Unix()
			//上次通信时间+心跳时间 > 当前时间则超时
			if atomic.LoadInt64(&a.lastAt) < deadline {
				logger.Log.Debugf("Session heartbeat timeout, LastTime=%d, Deadline=%d", atomic.LoadInt64(&a.lastAt), deadline)
				return
			}
			if _, err := a.conn.Write(hbd); err != nil {
				return //发送心跳消息
			}
		case <-a.chDie:
			return
		case <-a.chStopHeartbeat:
			return
		}
	}
}

func onSessionClosed(s *session.Session) {
	defer func() {
		if err := recover(); err != nil {
			logger.Log.Errorf("pitaya/onSessionClosed: %v", err)
		}
	}()

	for _, fn1 := range s.OnCloseCallbacks {
		fn1()
	}

	for _, fn2 := range session.SessionCloseCallbacks {
		fn2(s)
	}
}

// SendHandshakeResponse sends a handshake response
func (a *Agent) SendHandshakeResponse() error {
	_, err := a.conn.Write(hrd)
	return err
}

// 从send管道读取要发送的消息 然后通过
func (a *Agent) write() {
	// clean func
	defer func() {
		close(a.chSend)
		a.Close()
	}()

	for {
		select {
		case data := <-a.chSend: //读取管道
			//使用Serializer序列化
			payload, err := util.SerializeOrRaw(a.serializer, data.payload)
			if err != nil {
				logger.Log.Errorf("Failed to serialize response: %s", err.Error())
				payload, err = util.GetErrorPayload(a.serializer, err)
				if err != nil {
					tracing.FinishSpan(data.ctx, err)
					if data.typ == message.Response {
						metrics.ReportTimingFromCtx(data.ctx, a.metricsReporters, handlerType, err)
					}
					logger.Log.Error("cannot serialize message and respond to the client ", err.Error())
					break
				}
			}

			// construct message and encode flag(preverse| msg type |route ) + message id + route + data
			m := &message.Message{
				Type:  data.typ,
				Data:  payload,
				Route: data.route,
				ID:    data.mid,
				Err:   data.err,
			}
			//封装消息头处理路由消息id 压缩消息体 flag(preserve+msg.type+rout flag) + msg id + route + zip payload data
			em, err := a.messageEncoder.Encode(m)
			if err != nil {
				tracing.FinishSpan(data.ctx, err)
				if data.typ == message.Response {
					metrics.ReportTimingFromCtx(data.ctx, a.metricsReporters, handlerType, err)
				}
				logger.Log.Errorf("Failed to encode message: %s", err.Error())
				break
			}

			// packet encode 将message序列化的二进制封装使用packet进行序列化 packet.ytpe(1btyte)+body lenght(3byte)+bodydata
			p, err := a.encoder.Encode(packet.Data, em)
			if err != nil {
				tracing.FinishSpan(data.ctx, err)
				if data.typ == message.Response {
					metrics.ReportTimingFromCtx(data.ctx, a.metricsReporters, handlerType, err)
				}
				logger.Log.Errorf("Failed to encode packet: %s", err.Error())
				break
			}
			// close agent if low-level Conn broken
			if _, err := a.conn.Write(p); err != nil {
				tracing.FinishSpan(data.ctx, err)
				if data.typ == message.Response {
					metrics.ReportTimingFromCtx(data.ctx, a.metricsReporters, handlerType, err)
				}
				logger.Log.Errorf("Failed to write response: %s", err.Error())
				return
			}
			var e error
			tracing.FinishSpan(data.ctx, e)
			if data.typ == message.Response {
				var rErr error
				if m.Err {
					rErr = util.GetErrorFromPayload(a.serializer, payload)
				}
				metrics.ReportTimingFromCtx(data.ctx, a.metricsReporters, handlerType, rErr)
			}
		case <-a.chStopWrite:
			return
		}
	}
}

// SendRequest sends a request to a server
func (a *Agent) SendRequest(ctx context.Context, serverID, route string, v interface{}) (*protos.Response, error) {
	return nil, e.New("not implemented")
}

// AnswerWithError answers with an error
func (a *Agent) AnswerWithError(ctx context.Context, mid uint, err error) {
	if ctx != nil && err != nil {
		s := opentracing.SpanFromContext(ctx)
		if s != nil {
			tracing.LogError(s, err.Error())
		}
	}
	p, e := util.GetErrorPayload(a.serializer, err)
	if e != nil {
		logger.Log.Errorf("error answering the user with an error: %s", e.Error())
		return
	}
	e = a.Session.ResponseMID(ctx, mid, p, true)
	if e != nil {
		logger.Log.Errorf("error answering the user with an error: %s", e.Error())
	}
}

func hbdEncode(heartbeatTimeout time.Duration, packetEncoder codec.PacketEncoder, dataCompression bool, serializerName string) {
	//握手的内容为utf-8编码的json字符串
	hData := map[string]interface{}{
		"code": 200,
		"sys": map[string]interface{}{
			"heartbeat":  heartbeatTimeout.Seconds(),
			"dict":       message.GetDictionary(),
			"serializer": serializerName,
		},
	}
	data, err := gojson.Marshal(hData)
	if err != nil {
		panic(err)
	}

	if dataCompression {
		compressedData, err := compression.DeflateData(data)
		if err != nil {
			panic(err)
		}

		if len(compressedData) < len(data) {
			data = compressedData
		}
	}

	//hrd 握手的相应消息
	hrd, err = packetEncoder.Encode(packet.Handshake, data)
	if err != nil {
		panic(err)
	}

	//心跳包 length字段为0 body为空 hbd为encode之后的心跳包可以发送给conn的二进制数据
	hbd, err = packetEncoder.Encode(packet.Heartbeat, nil)
	if err != nil {
		panic(err)
	}
}

func (a *Agent) reportChannelSize() {
	chSendCapacity := a.messagesBufferSize - len(a.chSend)
	if chSendCapacity == 0 {
		logger.Log.Warnf("chSend is at maximum capacity")
	}
	for _, mr := range a.metricsReporters {
		if err := mr.ReportGauge(metrics.ChannelCapacity, map[string]string{"channel": "agent_chsend"}, float64(chSendCapacity)); err != nil {
			logger.Log.Warnf("failed to report chSend channel capaacity: %s", err.Error())
		}
	}
}
