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

package component

import (
	"errors"
	"reflect"

	"github.com/topfreegames/pitaya/conn/message"
	"github.com/topfreegames/pitaya/constants"
)

type (
	//Handler represents a message.Message's handler's meta information.
	//Component中Handler方法的反射信息
	Handler struct {
		Receiver    reflect.Value  // component的信息
		Method      reflect.Method // method stub 方法信息
		Type        reflect.Type   // 反射变量的类型 reflect.TypeOf(xxx)
		IsRawArg    bool           // 是否未经序列化的消息
		MessageType message.Type   // server接收的请求的客户端消息类型 request notify
	}

	//Remote represents remote's meta information.
	//远程组件的Handler的方法反射信息
	Remote struct {
		Receiver reflect.Value  // component的信息
		Method   reflect.Method //  rpc方法
		Type     reflect.Type   //  rpc参数的类型化信息
		HasArgs  bool           //
	}

	// Service implements a specific service, some of it's methods will be
	// called when the correspond events is occurred.
	//以组件为单位的Component的handler的反射信息的记录 一个Component 一个Service
	//一个组件的反射信息和所有的Handler的反射信息
	Service struct {
		Name     string              // name of service 服务的名字组件名字
		Type     reflect.Type        // type of the receiver  组件的类型信息
		Receiver reflect.Value       // receiver of methods for the service 组件的值信息
		Handlers map[string]*Handler // registered methods 所有本地方法Handler信息 和名字的map
		Remotes  map[string]*Remote  // registered remote methods 所有远程方法的方法名和Remote的map
		Options  options             // options
	}
)

// NewService creates a new service
func NewService(comp Component, opts []Option) *Service {
	//记录下组件的TypeOf和ValueOf
	s := &Service{
		Type:     reflect.TypeOf(comp),
		Receiver: reflect.ValueOf(comp),
	}

	// apply options 使用选项处理函数Option对选项Options进行处理 例如withname 是给s.name赋值
	for i := range opts {
		opt := opts[i] //Option func(options *options)
		opt(&s.Options)
	}
	if name := s.Options.name; name != "" {
		s.Name = name
	} else {
		s.Name = reflect.Indirect(s.Receiver).Type().Name() //reflect.Indirect 获取s.Receiver的值相当于value.Elem() 呼气s.Receiver的的type 再获取名字
	}

	return s
}

// ExtractHandler extract the set of methods from the
// receiver value which satisfy the following conditions:
// - exported method of exported type
// - one or two arguments
// - the first argument is context.Context
// - the second argument (if it exists) is []byte or a pointer
// - zero or two outputs
// - the first output is [] or a pointer
// - the second output is an error
// 将组件中符合Handler格式的方法信息封装为Handler同时和名字对应存储为map形式 放入Handlers结构
func (s *Service) ExtractHandler() error {
	typeName := reflect.Indirect(s.Receiver).Type().Name()
	if typeName == "" {
		return errors.New("no service name for type " + s.Type.String())
	}
	if !isExported(typeName) {
		return errors.New("type " + typeName + " is not exported")
	}

	// Install the methods
	//遍历S.Type的method将符合Handler类型的方法的信息封装为封装为Handler同时和名字对应存储为map
	s.Handlers = suitableHandlerMethods(s.Type, s.Options.nameFunc) //s.Options.nameFunc用来处理方法名 转化大小写等

	if len(s.Handlers) == 0 {
		str := ""
		// To help the user, see if a pointer receiver would work.
		method := suitableHandlerMethods(reflect.PtrTo(s.Type), s.Options.nameFunc)
		if len(method) != 0 {
			str = "type " + s.Name + " has no exported methods of handler type (hint: pass a pointer to value of that type)"
		} else {
			str = "type " + s.Name + " has no exported methods of handler type"
		}
		return errors.New(str)
	}

	for i := range s.Handlers {
		s.Handlers[i].Receiver = s.Receiver //receiver机方法的绑定的对象（调用者component的信息）
	}

	return nil
}

// ExtractRemote extract the set of methods from the
// receiver value which satisfy the following conditions:
// - exported method of exported type
// - two return values
// - the first return implements protobuf interface
// - the second return is an error
func (s *Service) ExtractRemote() error {
	typeName := reflect.Indirect(s.Receiver).Type().Name()
	if typeName == "" {
		return errors.New("no service name for type " + s.Type.String())
	}
	if !isExported(typeName) {
		return errors.New("type " + typeName + " is not exported")
	}

	// Install the methods
	//遍历远程方法 生成Handler信息
	s.Remotes = suitableRemoteMethods(s.Type, s.Options.nameFunc)

	if len(s.Remotes) == 0 {
		str := ""
		// To help the user, see if a pointer receiver would work.
		method := suitableRemoteMethods(reflect.PtrTo(s.Type), s.Options.nameFunc)
		if len(method) != 0 {
			str = "type " + s.Name + " has no exported methods of remote type (hint: pass a pointer to value of that type)"
		} else {
			str = "type " + s.Name + " has no exported methods of remote type"
		}
		return errors.New(str)
	}

	for i := range s.Remotes {
		s.Remotes[i].Receiver = s.Receiver //component的信息
	}
	return nil
}

// ValidateMessageType validates a given message type against the handler's one
// and returns an error if it is a mismatch and a boolean indicating if the caller should
// exit in the presence of this error or not.
func (h *Handler) ValidateMessageType(msgType message.Type) (exitOnError bool, err error) {
	if h.MessageType != msgType {
		switch msgType {
		case message.Request:
			err = constants.ErrRequestOnNotify
			exitOnError = true

		case message.Notify:
			err = constants.ErrNotifyOnRequest
		}
	}
	return
}
