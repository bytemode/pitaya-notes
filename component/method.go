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
	"context"
	"reflect"
	"unicode"
	"unicode/utf8"

	"github.com/golang/protobuf/proto"
	"github.com/topfreegames/pitaya/conn/message"
)

//处理组件中的本地消息处理方法封装为Handler， 将组件中的rpc调用方法封装为Remote，
//并且将方法名字和Handler和Remote进行map存储
//名字经过option.nameFunc处理

var (
	typeOfError    = reflect.TypeOf((*error)(nil)).Elem()        //error类型
	typeOfBytes    = reflect.TypeOf(([]byte)(nil))               //[]byte类型
	typeOfContext  = reflect.TypeOf(new(context.Context)).Elem() //context.Context类型
	typeOfProtoMsg = reflect.TypeOf(new(proto.Message)).Elem()   //proto.Message类型
)

//通过首字母大写判断导出情况
func isExported(name string) bool {
	w, _ := utf8.DecodeRuneInString(name) //返回一个人rune 和 rune 对应的utf8编码字节数
	return unicode.IsUpper(w)
}

// isRemoteMethod decide a method is suitable remote method
// 是rpc过程调用的方法类型 带参数或者不带参数 类型必须是proto.Message类型类型的指针 返回值必须是proto.Message类型指针和error
func isRemoteMethod(method reflect.Method) bool {
	mt := method.Type
	// Method must be exported.
	if method.PkgPath != "" {
		return false
	}

	// Method needs at least two ins: receiver and context.Context
	// 有一个或者两个参数
	if mt.NumIn() != 2 && mt.NumIn() != 3 {
		return false
	}

	//第1个参数要是context.Context类型
	if t1 := mt.In(1); !t1.Implements(typeOfContext) { //判断第二个参数是否context类型
		return false
	}

	//第二个参数需要是proto.Message类型即rpc调用的函数参数
	if mt.NumIn() == 3 {
		if t2 := mt.In(2); !t2.Implements(typeOfProtoMsg) { ////判断第三个参数是否proto.Message类型
			return false
		}
	}

	// Method needs two outs: interface{}(that implements proto.Message), error
	if mt.NumOut() != 2 { //判断返回值的数量
		return false
	}

	if (mt.Out(0).Kind() != reflect.Ptr) || mt.Out(1) != typeOfError {
		return false //第一个返回值是指针 第二个返回值是是error
	}

	if o0 := mt.Out(0); !o0.Implements(typeOfProtoMsg) { //返回值的第1个参数需要是proto.Message类型
		return false
	}

	return true
}

// isHandlerMethod decide a method is suitable handler method
// 如果是符合本地消息响应的类型Handler类型的方法则返回true
// 有一个或者两个参数 第一个是context.Context 第二个是[]byte or ptr 有0个或者两个返回值 2个的时候是 ptr和error
// func (r *Room) Join(ctx context.Context) (*JoinResponse, error)
// func (r *Room) Join(ctx context.Context, []byte) (*JoinResponse, error)
func isHandlerMethod(method reflect.Method) bool {
	mt := method.Type
	// Method must be exported.
	if method.PkgPath != "" {
		return false
	}

	// Method needs two or three ins: receiver, context.Context and optional []byte or pointer.
	if mt.NumIn() != 2 && mt.NumIn() != 3 { //判断方法的参数 需要有两个或者三个参数 receivcer context 第三个参数是[]byte 或者 *proto.message
		return false
	}

	if t1 := mt.In(1); !t1.Implements(typeOfContext) {
		return false //第二个参数是context.Context
	}

	if mt.NumIn() == 3 && mt.In(2).Kind() != reflect.Ptr && mt.In(2) != typeOfBytes {
		return false //第三个参数是ptr或者[]byte
	}

	// Method needs either no out or two outs: interface{}(or []byte), error
	if mt.NumOut() != 0 && mt.NumOut() != 2 {
		return false //有两个或0个返回值
	}

	if mt.NumOut() == 2 && (mt.Out(1) != typeOfError || mt.Out(0) != typeOfBytes && mt.Out(0).Kind() != reflect.Ptr) {
		return false //返回值有两个 第一个是[]byte或者指针 第二个是error
	}

	return true
}

// 将组件中所有的符合rpc方法类型的方法 进行信息封装到Remote并且和方法名map存储
func suitableRemoteMethods(typ reflect.Type, nameFunc func(string) string) map[string]*Remote {
	methods := make(map[string]*Remote)
	for m := 0; m < typ.NumMethod(); m++ { //遍历方法
		method := typ.Method(m)
		mt := method.Type
		mn := method.Name //取出方法名
		if isRemoteMethod(method) {
			// rewrite remote name
			if nameFunc != nil {
				mn = nameFunc(mn) //处理方法名
			}
			//构建远程方法调用新消息 rpc调用信息
			methods[mn] = &Remote{
				Method:  method,
				HasArgs: method.Type.NumIn() == 3, //是否有参数 prc的参数
			}
			if mt.NumIn() == 3 {
				methods[mn].Type = mt.In(2) //方法的第二个输入参数的类型消息的类型参数
			}
		}
	}
	return methods
}

// 将组件中所有的符合本地消息响应方法类型的方法 进行信息封装到Handler并且和方法名map存储
func suitableHandlerMethods(typ reflect.Type, nameFunc func(string) string) map[string]*Handler {
	methods := make(map[string]*Handler)
	//根据方法的数量取出方法 遍历所有的方法
	for m := 0; m < typ.NumMethod(); m++ {
		method := typ.Method(m)
		mt := method.Type            //方法的类型信息
		mn := method.Name            //方法名
		if isHandlerMethod(method) { //复合接收消息的格式的函数为Handler 分为通知消息和请求消息两种（通知无返回值请求有两个返回值）
			raw := false
			if mt.NumIn() == 3 && mt.In(2) == typeOfBytes {
				raw = true //消息是否原始字节消息
			}
			// rewrite handler name
			if nameFunc != nil {
				mn = nameFunc(mn) //对方法名进行处理 是通过options中的函数进行处理
			}
			var msgType message.Type
			if mt.NumOut() == 0 {
				msgType = message.Notify //没有返回值的是Nottfy
			} else {
				msgType = message.Request //有返回值的是Request
			}
			// 封装方法调用的信息（方法反射信息 是否原始消息 消息类型（handler返回值））
			handler := &Handler{
				Method:      method,  //方法
				IsRawArg:    raw,     //消息是否未序列化
				MessageType: msgType, //request notify
			}
			if mt.NumIn() == 3 {
				handler.Type = mt.In(2)
			}
			methods[mn] = handler //名字和类型信息对应存储
		}
	}
	return methods
}
