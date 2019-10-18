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

package acceptorwrapper

import (
	"net"

	"github.com/topfreegames/pitaya/acceptor"
)

//接收器包装器 包装器可以包装接收器，以执行消息转发之前读取和更改传入的数据
//创建包装器需要从BaseWrapper继承或者实现Wrapper接口然后使用WithWrappers将其添加到接收器中

// BaseWrapper implements Wrapper by saving the acceptor as an attribute.
// Conns from acceptor.GetConnChan are processed by wrapConn and
// forwarded to its own connChan.
// Any new wrapper can inherit from BaseWrapper and just implement wrapConn.
type BaseWrapper struct {
	acceptor.Acceptor //acceptor.Acceptor接口作为BaseWrapper的匿名成员 BaseWrapper即为Acceptor的实现 而不用实现接口所有方法
	connChan          chan net.Conn
	wrapConn          func(net.Conn) net.Conn
}

//把interface作为struct的一个匿名成员，就可以假设struct就是此成员interface的一个实现，而不管struct是否已经实现interface所定义的函数。

// NewBaseWrapper returns an instance of BaseWrapper.
func NewBaseWrapper(wrapConn func(net.Conn) net.Conn) BaseWrapper {
	return BaseWrapper{
		connChan: make(chan net.Conn), //无缓冲chan读写会阻塞 make(chan net.Conn, 1) 有缓冲chan不会阻塞
		wrapConn: wrapConn,
	}
}

// ListenAndServe starts a goroutine that wraps acceptor's conn
// and calls acceptor's listenAndServe
func (b *BaseWrapper) ListenAndServe() {
	go b.pipe()
	b.Acceptor.ListenAndServe()
}

// GetConnChan returns the wrapper conn chan
func (b *BaseWrapper) GetConnChan() chan net.Conn {
	return b.connChan
}

func (b *BaseWrapper) pipe() {
	for conn := range b.Acceptor.GetConnChan() { //等待并且从chan中读取数据直到chan关闭且数据全部读出之后for range结束
		b.connChan <- b.wrapConn(conn)
	}

	// for {
	// 	if conn, ok := <-b.Acceptor.GetConnChan(); ok {
	// 		b.connChan <- b.wrapConn(conn)
	// 	} else {
	// 		break
	// 	}
	// }
}
