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
	"github.com/topfreegames/pitaya/acceptor"
)

//接收器包装器 包装器可以包装接收器，以执行消息转发之前读取和更改传入的数据
//创建包装器需要从BaseWrapper继承或者实现Wrapper接口然后使用WithWrappers将其添加到接收器中

// Wrapper has a method that receives an acceptor and the struct
// that implements must encapsulate it. The main goal is to create
// a middleware for packets of net.Conn from acceptor.GetConnChan before
// giving it to serviceHandler.
type Wrapper interface {
	Wrap(acceptor.Acceptor) acceptor.Acceptor
}

// WithWrappers walks through wrappers calling Wrapper
// wrappers ...Wrapper 不确定参数  wapers ... 切片打算传递
// for k,v:= range  array slice map(随机) string chan(v) for循环中，如果循环变量不是指针，那么每次的变量是同一个，不过值变了
func WithWrappers(
	a acceptor.Acceptor,
	wrappers ...Wrapper,
) acceptor.Acceptor {
	for _, w := range wrappers {
		a = w.Wrap(a)
	}
	return a
}
