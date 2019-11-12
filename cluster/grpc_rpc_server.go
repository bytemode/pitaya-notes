// Copyright (c) TFG Co. All Rights Reserved.
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

package cluster

import (
	"fmt"
	"net"

	"google.golang.org/grpc"

	"github.com/topfreegames/pitaya/config"
	"github.com/topfreegames/pitaya/metrics"
	"github.com/topfreegames/pitaya/protos"
)

// GRPCServer rpc server struct
type GRPCServer struct {
	server           *Server             //本地当前服务器
	config           *config.Config      //vipper配置
	grpcSv           *grpc.Server        // grpc server
	pitayaServer     protos.PitayaServer //grpc server 生成协议
	metricsReporters []metrics.Reporter
}

// NewGRPCServer constructor
func NewGRPCServer(config *config.Config, server *Server, metricsReporters []metrics.Reporter) (*GRPCServer, error) {
	gs := &GRPCServer{
		config:           config,
		server:           server,
		metricsReporters: metricsReporters,
	}
	return gs, nil
}

// Init inits grpc rpc server
// 在grpc中注册我们的服务实现并且启动监听开始grpc服务
func (gs *GRPCServer) Init() error {
	port := gs.config.GetInt("pitaya.cluster.rpc.server.grpc.port")
	//监听端口
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	//实例化grpc Server
	gs.grpcSv = grpc.NewServer()

	//在 gRPC 服务器注册我们的服务实现 gs.pitayaServer === remoteService 即remoteService实现了grpc服务器的方法
	protos.RegisterPitayaServer(gs.grpcSv, gs.pitayaServer) //gs.grpcSv.RegisterService(&_Pitaya_serviceDesc, gs.pitayaServer)
	//启动grpc服务 启动监听 accept conn go routine处理连接请求
	go gs.grpcSv.Serve(lis)
	return nil
}

// SetPitayaServer sets the pitaya server 设置grpc具体实现的服务接口 基本上都是remoteService
func (gs *GRPCServer) SetPitayaServer(ps protos.PitayaServer) {
	gs.pitayaServer = ps
}

// AfterInit runs after initialization
func (gs *GRPCServer) AfterInit() {}

// BeforeShutdown runs before shutdown
func (gs *GRPCServer) BeforeShutdown() {}

// Shutdown stops grpc rpc server
func (gs *GRPCServer) Shutdown() error {
	// graceful: stops the server from accepting new connections and RPCs and
	// blocks until all the pending RPCs are finished.
	// source: https://godoc.org/google.golang.org/grpc#Server.GracefulStop
	gs.grpcSv.GracefulStop() //优雅的退出
	return nil
}
