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

package context

import (
	"context"
	"encoding/json"

	"github.com/topfreegames/pitaya/constants"
)

//上下文  多goroutine共享数据和多goroutine管理机制
// type Context interface {
//     Deadline() (deadline time.Time, ok bool)
//     Done() <-chan struct{}
//     Err() error
//     Value(key interface{}) interface{}
// }
// Deadline 会返回一个超时时间，Goroutine获得了超时时间后，例如可以对某些io操作设定超时时间。
// Done方   法返回一个信道（channel），当Context被撤销或过期时，该信道是关闭的，即它是一个表示Context是否已关闭的信号。
// 当Done信道关闭后，Err方法表明Context被撤的原因。
// Value可以让Goroutine共享一些数据，当然获得数据是协程安全的。但使用这些数据的时候要注意同步，比如返回了一个map，而这个map的读写则要加锁
// Context结构也应该像一棵树，叶子节点须总是由根节点衍生出来的, 顶部的Goroutine应有办法主动关闭其下属的Goroutine的执行
//context.Background返回根节点,返回空的context 它不能被取消、没有值、也没有过期时间func WithCancel(parent Context) (ctx Context, cancel CancelFunc)
// 创建子孙节点
// func WithCancel(parent Context) (ctx Context, cancel CancelFunc) 调用CancelFunc对象将撤销对应的Context对象
// func WithDeadline(parent Context, deadline time.Time) (Context, CancelFunc) 过期时间由deadline和parent的过期时间共同决定
// func WithTimeout(parent Context, timeout time.Duration) (Context, CancelFunc)  传入的是从现在开始Context剩余的生命时长
// func WithValue(parent Context, key interface{}, val interface{}) Context 插入值使用Value(key)获取
//子节点使用如下方法判断退出:
// select {
// case <-cxt.Done():
// 	// do some clean...
// }

//向传播context中添加数据 key value

// AddToPropagateCtx adds a key and value that will be propagated through RPC calls
func AddToPropagateCtx(ctx context.Context, key string, val interface{}) context.Context {
	propagate := ToMap(ctx) //取出ctx中的 constants.PropagateCtxKey所对应的map[string]interface{}{}
	propagate[key] = val
	return context.WithValue(ctx, constants.PropagateCtxKey, propagate)
}

// GetFromPropagateCtx get a value from the propagate
func GetFromPropagateCtx(ctx context.Context, key string) interface{} {
	propagate := ToMap(ctx)
	if val, ok := propagate[key]; ok {
		return val
	}
	return nil
}

// ToMap returns the values that will be propagated through RPC calls in map[string]interface{} format
func ToMap(ctx context.Context) map[string]interface{} {
	if ctx == nil {
		return map[string]interface{}{}
	}
	p := ctx.Value(constants.PropagateCtxKey)
	if p != nil {
		return p.(map[string]interface{})
	}
	return map[string]interface{}{}
}

// FromMap creates a new context from a map with propagated values
func FromMap(val map[string]interface{}) context.Context {
	return context.WithValue(context.Background(), constants.PropagateCtxKey, val)
}

// Encode returns the given propagatable context encoded in binary format
func Encode(ctx context.Context) ([]byte, error) {
	m := ToMap(ctx)
	if len(m) > 0 {
		return json.Marshal(m)
	}
	return nil, nil
}

// Decode returns a context given a binary encoded message
func Decode(m []byte) (context.Context, error) {
	if len(m) == 0 {
		// TODO maybe return an error
		return nil, nil
	}
	mp := make(map[string]interface{}, 0)
	err := json.Unmarshal(m, &mp)
	if err != nil {
		return nil, err
	}
	return FromMap(mp), nil
}
