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

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/google/uuid"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/topfreegames/pitaya/agent"
	"github.com/topfreegames/pitaya/cluster"
	"github.com/topfreegames/pitaya/component"
	"github.com/topfreegames/pitaya/conn/codec"
	"github.com/topfreegames/pitaya/conn/message"
	"github.com/topfreegames/pitaya/conn/packet"
	"github.com/topfreegames/pitaya/constants"
	pcontext "github.com/topfreegames/pitaya/context"
	"github.com/topfreegames/pitaya/docgenerator"
	e "github.com/topfreegames/pitaya/errors"
	"github.com/topfreegames/pitaya/logger"
	"github.com/topfreegames/pitaya/metrics"
	"github.com/topfreegames/pitaya/route"
	"github.com/topfreegames/pitaya/serialize"
	"github.com/topfreegames/pitaya/session"
	"github.com/topfreegames/pitaya/timer"
	"github.com/topfreegames/pitaya/tracing"
)

var (
	handlers    = make(map[string]*component.Handler) // 所有的组件中的Handler和名字的map
	handlerType = "handler"
)

type (
	// HandlerService service 管理所有组件生成的Service 同时处理连接上的消息使用hander进行处理分发 也处理定时任务更新
	HandlerService struct {
		appDieChan         chan bool             // die channel app
		chLocalProcess     chan unhandledMessage // channel of messages that will be processed locally
		chRemoteProcess    chan unhandledMessage // channel of messages that will be processed remotely
		decoder            codec.PacketDecoder   // binary decoder
		encoder            codec.PacketEncoder   // binary encoder
		heartbeatTimeout   time.Duration
		messagesBufferSize int
		remoteService      *RemoteService
		serializer         serialize.Serializer          // message serializer
		server             *cluster.Server               // server obj
		services           map[string]*component.Service // 所有注册的组件的服务信息, Service存储着component的反射信息Handler信息
		messageEncoder     message.Encoder
		metricsReporters   []metrics.Reporter
	}

	unhandledMessage struct {
		ctx   context.Context
		agent *agent.Agent
		route *route.Route
		msg   *message.Message
	}
)

// NewHandlerService creates and returns a new handler service
func NewHandlerService(
	dieChan chan bool,
	packetDecoder codec.PacketDecoder,
	packetEncoder codec.PacketEncoder,
	serializer serialize.Serializer,
	heartbeatTime time.Duration,
	messagesBufferSize,
	localProcessBufferSize,
	remoteProcessBufferSize int,
	server *cluster.Server,
	remoteService *RemoteService,
	messageEncoder message.Encoder,
	metricsReporters []metrics.Reporter,
) *HandlerService {
	h := &HandlerService{
		services:           make(map[string]*component.Service),
		chLocalProcess:     make(chan unhandledMessage, localProcessBufferSize),
		chRemoteProcess:    make(chan unhandledMessage, remoteProcessBufferSize),
		decoder:            packetDecoder,
		encoder:            packetEncoder,
		messagesBufferSize: messagesBufferSize,
		serializer:         serializer,
		heartbeatTimeout:   heartbeatTime,
		appDieChan:         dieChan,
		server:             server,
		remoteService:      remoteService,
		messageEncoder:     messageEncoder,
		metricsReporters:   metricsReporters,
	}

	return h
}

// Dispatch message to corresponding logic handler
//app启动的时候启动多个goroutine调用Dispatch来处理消息和定时任务
func (h *HandlerService) Dispatch(thread int) {
	// TODO: This timer is being stopped multiple times, it probably doesn't need to be stopped here
	defer timer.GlobalTicker.Stop()

	for {
		// Calls to remote servers block calls to local server
		select {
		case lm := <-h.chLocalProcess: //处理本地服务message
			metrics.ReportMessageProcessDelayFromCtx(lm.ctx, h.metricsReporters, "local")
			h.localProcess(lm.ctx, lm.agent, lm.route, lm.msg)

		case rm := <-h.chRemoteProcess: //处理远端服务message
			metrics.ReportMessageProcessDelayFromCtx(rm.ctx, h.metricsReporters, "remote")
			h.remoteService.remoteProcess(rm.ctx, nil, rm.agent, rm.route, rm.msg)

		case <-timer.GlobalTicker.C: // execute cron task
			timer.Cron()

		case t := <-timer.Manager.ChCreatedTimer: // new Timers
			timer.AddTimer(t)

		case id := <-timer.Manager.ChClosingTimer: // closing Timers
			timer.RemoveTimer(id)
		}
	}
}

// Register registers components
//将组件Component中的复合Handler的方法遍历处理按照路由名字生成一个map放入HandleServices
// 一个组件生成一个Service存储组件本身和组件中handler的反射信息 同时将组件名字和service关联 且记录所有的路由和Handle的反射信息
func (h *HandlerService) Register(comp component.Component, opts []component.Option) error {
	//内部创还能一个Service存储component反射信息，和处理service选项信息的函数
	s := component.NewService(comp, opts)

	if _, ok := h.services[s.Name]; ok {
		return fmt.Errorf("handler: service already defined: %s", s.Name)
	}

	// 将组件方法进行遍历取出符合条件的本地方法（消息处理方法）封装成为Handler, 并且存储为map[method-name]Handler结构
	if err := s.ExtractHandler(); err != nil {
		return err
	}

	//一个组件的Handler生成反射信息记录在Service 通过组件的名字进行映射记录
	h.services[s.Name] = s

	//注册所有的Handler map[service-name.memthod-name]Handler的形式
	for name, handler := range s.Handlers {
		handlers[fmt.Sprintf("%s.%s", s.Name, name)] = handler //组件名.方法名 和handler（方法的反射信息）的映射
	}
	return nil
}

// Handle handles messages from a conn
//app 启动之后 对每个acceptor上管道上传来的conn 创建一个agent然后读取数据成成消息写入消息管道
func (h *HandlerService) Handle(conn net.Conn) {
	// create a client agent and startup write goroutine  agent用来处理心跳和给客户端发送消息
	a := agent.NewAgent(conn, h.decoder, h.encoder, h.serializer, h.heartbeatTimeout, h.messagesBufferSize, h.appDieChan, h.messageEncoder, h.metricsReporters)

	// startup agent goroutine
	go a.Handle()

	logger.Log.Debugf("New session established: %s", a.String())

	// guarantee agent related resource is destroyed
	defer func() {
		a.Session.Close()
		logger.Log.Debugf("Session read goroutine exit, SessionID=%d, UID=%d", a.Session.ID(), a.Session.UID())
	}()

	// read loop 循环读取网络连接上的消息
	buf := make([]byte, 2048)
	for {
		n, err := conn.Read(buf) //读取数据放入buf返回字节数 超过buf会自动扩容
		if err != nil {
			logger.Log.Debugf("Read message error: '%s', session will be closed immediately", err.Error())
			return
		}

		logger.Log.Debug("Received data on connection")

		//从buf中取出n字节使用codec.Decoder做packet层的解析会根据 1byte+3byte+data的个数读取数据返回一个packet切片，此处为拆包
		// (warning): decoder uses slice for performance, packet data should be copied before next Decode
		packets, err := h.decoder.Decode(buf[:n]) //packet解码得到多个packet type type 1btyte+ body length 3byte + body data
		if err != nil {
			logger.Log.Errorf("Failed to decode message: %s", err.Error())
			return
		}

		//如果数据包不够一个大小则继续read网络数据
		if len(packets) < 1 {
			logger.Log.Warnf("Read no packets, data: %v", buf[:n])
			continue
		}

		// process all packet 处理所有packet 根据不同的packet.type进行处理
		for i := range packets {
			if err := h.processPacket(a, packets[i]); err != nil {
				logger.Log.Errorf("Failed to process packet: %s", err.Error())
				return
			}
		}
	}
}

func (h *HandlerService) processPacket(a *agent.Agent, p *packet.Packet) error {
	switch p.Type {
	case packet.Handshake: // 收到握手请求
		logger.Log.Debug("Received handshake packet")
		if err := a.SendHandshakeResponse(); err != nil { //回复握手相应协议
			logger.Log.Errorf("Error sending handshake response: %s", err.Error())
			return err
		}
		logger.Log.Debugf("Session handshake Id=%d, Remote=%s", a.Session.ID(), a.RemoteAddr())

		// Parse the json sent with the handshake by the client
		handshakeData := &session.HandshakeData{}
		err := json.Unmarshal(p.Data, handshakeData) //packet.data反序列化为 session.HandshakeData
		if err != nil {
			a.SetStatus(constants.StatusClosed)
			return fmt.Errorf("Invalid handshake data. Id=%d", a.Session.ID())
		}

		//session关联handshakedata
		a.Session.SetHandshakeData(handshakeData)
		a.SetStatus(constants.StatusHandshake) //agent进入握手状态
		err = a.Session.Set(constants.IPVersionKey, a.IPVersion())
		if err != nil {
			logger.Log.Warnf("failed to save ip version on session: %q\n", err)
		}

		logger.Log.Debug("Successfully saved handshake data")

	case packet.HandshakeAck: //回复客户端握手请求之后，客户端会回复act agent进入 work
		a.SetStatus(constants.StatusWorking)
		logger.Log.Debugf("Receive handshake ACK Id=%d, Remote=%s", a.Session.ID(), a.RemoteAddr())

	case packet.Data: //收到的是数据包 request或者notify类型
		if a.GetStatus() < constants.StatusWorking {
			return fmt.Errorf("receive data on socket which is not yet ACK, session will be closed immediately, remote=%s",
				a.RemoteAddr().String())
		}

		//将packet的data部分交给message解码 获取到 flag(preserve tyep flag)+id+route+data
		///flag中可以获取到type（request或者notify）和route压缩和gzip压缩 地根据此字段读取出routestr 之后的就是纯数据部分
		msg, err := message.Decode(p.Data)
		if err != nil {
			return err
		}
		h.processMessage(a, msg) //处理message

	case packet.Heartbeat:
		// expected 心跳
	}

	a.SetLastAt()
	return nil
}

func (h *HandlerService) processMessage(a *agent.Agent, msg *message.Message) {
	requestID := uuid.New()
	//ctx中记录start time \route\requestud
	ctx := pcontext.AddToPropagateCtx(context.Background(), constants.StartTimeKey, time.Now().UnixNano())
	ctx = pcontext.AddToPropagateCtx(ctx, constants.RouteKey, msg.Route)
	ctx = pcontext.AddToPropagateCtx(ctx, constants.RequestIDKey, requestID.String())
	tags := opentracing.Tags{
		"local.id":   h.server.ID,
		"span.kind":  "server",
		"msg.type":   strings.ToLower(msg.Type.String()),
		"user.id":    a.Session.UID(),
		"request.id": requestID.String(),
	}
	ctx = tracing.StartSpan(ctx, msg.Route, tags)
	ctx = context.WithValue(ctx, constants.SessionCtxKey, a.Session)

	//解析route变为svtype service method
	r, err := route.Decode(msg.Route)
	if err != nil {
		logger.Log.Errorf("Failed to decode route: %s", err.Error())
		a.AnswerWithError(ctx, msg.ID, e.NewError(err, e.ErrBadRequestCode))
		return
	}

	if r.SvType == "" {
		r.SvType = h.server.Type
	}

	//构建未处理消息
	message := unhandledMessage{
		ctx:   ctx,
		agent: a,
		route: r,
		msg:   msg,
	}

	//本地消息则交由本地服务器处理 写入本地process chan
	if r.SvType == h.server.Type {
		h.chLocalProcess <- message
	} else {
		//如果不是本地服务器则交由远程服务器处理 发送到远程管道
		if h.remoteService != nil {
			h.chRemoteProcess <- message
		} else {
			logger.Log.Warnf("request made to another server type but no remoteService running")
		}
	}
}

//处理本地
func (h *HandlerService) localProcess(ctx context.Context, a *agent.Agent, route *route.Route, msg *message.Message) {
	var mid uint
	switch msg.Type {
	case message.Request: //如果是request则记录mid
		mid = msg.ID
	case message.Notify:
		mid = 0
	}

	//根据route查找Handler然后利用反射进行调用
	ret, err := processHandlerMessage(ctx, route, h.serializer, a.Session, msg.Data, msg.Type, false)
	if msg.Type != message.Notify {
		if err != nil {
			logger.Log.Errorf("Failed to process handler message: %s", err.Error())
			a.AnswerWithError(ctx, mid, err)
		} else {
			a.Session.ResponseMID(ctx, mid, ret) //request才需要response调用的是agent的ResponseMID
		}
	} else {
		metrics.ReportTimingFromCtx(ctx, h.metricsReporters, handlerType, nil)
		tracing.FinishSpan(ctx, err)
	}
}

// DumpServices outputs all registered services
func (h *HandlerService) DumpServices() {
	for name := range handlers {
		logger.Log.Infof("registered handler %s, isRawArg: %s", name, handlers[name].IsRawArg)
	}
}

// Docs returns documentation for handlers
func (h *HandlerService) Docs(getPtrNames bool) (map[string]interface{}, error) {
	if h == nil {
		return map[string]interface{}{}, nil
	}
	return docgenerator.HandlersDocs(h.server.Type, h.services, getPtrNames)
}
