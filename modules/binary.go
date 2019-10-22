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

package modules

import (
	"bufio"
	"os/exec"
	"syscall"
	"time"

	"github.com/topfreegames/pitaya/constants"
	"github.com/topfreegames/pitaya/logger"
)

// Binary is a pitaya module that starts a binary as a child process and
// pipes its stdout
type Binary struct {
	Base
	binPath                  string
	args                     []string
	gracefulShutdownInterval time.Duration
	cmd                      *exec.Cmd
	exitCh                   chan struct{}
}

// NewBinary creates a new binary module with the given path
func NewBinary(binPath string, args []string, gracefulShutdownInterval ...time.Duration) *Binary {
	gracefulTime := 15 * time.Second
	if len(gracefulShutdownInterval) > 0 {
		gracefulTime = gracefulShutdownInterval[0]
	}
	return &Binary{
		binPath:                  binPath,
		args:                     args,
		gracefulShutdownInterval: gracefulTime,
		exitCh:                   make(chan struct{}),
	}
}

// GetExitChannel gets a channel that is closed when the binary dies
func (b *Binary) GetExitChannel() chan struct{} {
	return b.exitCh
}

// Init initializes the binary
func (b *Binary) Init() error {
	//exec 用来执行外部命令
	b.cmd = exec.Command(b.binPath, b.args...) //跟进执行文件名和参数返回一个cmd
	stdout, _ := b.cmd.StdoutPipe()            //返回一个管道，该管道会在Cmd中的命令被启动后连接到其标准输入
	stdOutScanner := bufio.NewScanner(stdout)  //带缓冲的io
	stderr, _ := b.cmd.StderrPipe()            //返回一个管道，该管道会在Cmd中的命令被启动后连接到其标准错误
	stdErrScanner := bufio.NewScanner(stderr)

	//循环读取标准输出和标准错误进行日志操作
	go func() {
		for stdOutScanner.Scan() {
			logger.Log.Info(stdOutScanner.Text())
		}
	}()
	go func() {
		for stdErrScanner.Scan() {
			logger.Log.Error(stdErrScanner.Text())
		}
	}()
	err := b.cmd.Start() ///执行Cmd中包含的命令，该方法立即返回，并不等待命令执行完成
	go func() {
		b.cmd.Wait()    //该方法会阻塞直到Cmd中的命令执行完成，但该命令必须是被Start方法开始执行的
		close(b.exitCh) //执行完毕后关闭exitCh chan
	}()
	return err
}

// Shutdown shutdowns the binary module
func (b *Binary) Shutdown() error {
	//给cmd执行程序发送结束信号
	err := b.cmd.Process.Signal(syscall.SIGTERM)
	if err != nil {
		return err
	}
	//SIGTERM
	timeout := time.After(b.gracefulShutdownInterval)
	select {
	case <-b.exitCh:
		return nil
	case <-timeout:
		b.cmd.Process.Kill() //sigterm超时则强制杀进程
		return constants.ErrTimeoutTerminatingBinaryModule
	}
}
