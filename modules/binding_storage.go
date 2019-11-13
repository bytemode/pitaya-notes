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
	"context"
	"fmt"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/namespace"
	"github.com/topfreegames/pitaya/cluster"
	"github.com/topfreegames/pitaya/config"
	"github.com/topfreegames/pitaya/constants"
	"github.com/topfreegames/pitaya/logger"
	"github.com/topfreegames/pitaya/session"
)

// ETCDBindingStorage module that uses etcd to keep in which frontend server each user is bound
// ETCD上存储了 bings/frontype/uid: frontserverid etcd上存储用户连接的前端服务器的serverid
type ETCDBindingStorage struct {
	Base
	config          *config.Config
	cli             *clientv3.Client //连接etcd的client
	etcdEndpoints   []string
	etcdPrefix      string
	etcdDialTimeout time.Duration    //etcd拨号超时
	leaseTTL        time.Duration    //租约超时
	leaseID         clientv3.LeaseID //租约id
	thisServer      *cluster.Server  //本地当前服务器
	stopChan        chan struct{}
}

// NewETCDBindingStorage returns a new instance of BindingStorage
func NewETCDBindingStorage(server *cluster.Server, conf *config.Config) *ETCDBindingStorage {
	b := &ETCDBindingStorage{
		config:     conf,
		thisServer: server,
		stopChan:   make(chan struct{}),
	}
	b.configure()
	return b
}

func (b *ETCDBindingStorage) configure() {
	b.etcdDialTimeout = b.config.GetDuration("pitaya.modules.bindingstorage.etcd.dialtimeout")
	b.etcdEndpoints = b.config.GetStringSlice("pitaya.modules.bindingstorage.etcd.endpoints")
	b.etcdPrefix = b.config.GetString("pitaya.modules.bindingstorage.etcd.prefix")
	b.leaseTTL = b.config.GetDuration("pitaya.modules.bindingstorage.etcd.leasettl")
}

//bindings/frontedtype/uid 组成key
func getUserBindingKey(uid, frontendType string) string {
	return fmt.Sprintf("bindings/%s/%s", frontendType, uid)
}

// PutBinding puts the binding info into etcd
// PutBinding 在etcd上存储bindings/frontedtype/uid : serverid
func (b *ETCDBindingStorage) PutBinding(uid string) error {
	_, err := b.cli.Put(context.Background(), getUserBindingKey(uid, b.thisServer.Type), b.thisServer.ID, clientv3.WithLease(b.leaseID))
	return err
}

//删除etcd上的key
func (b *ETCDBindingStorage) removeBinding(uid string) error {
	_, err := b.cli.Delete(context.Background(), getUserBindingKey(uid, b.thisServer.Type))
	return err
}

// GetUserFrontendID gets the id of the frontend server a user is connected to
// 获取用户(uid)连接的指定类型的前端服务器的id
//
func (b *ETCDBindingStorage) GetUserFrontendID(uid, frontendType string) (string, error) {
	etcdRes, err := b.cli.Get(context.Background(), getUserBindingKey(uid, frontendType))
	if err != nil {
		return "", err
	}
	if len(etcdRes.Kvs) == 0 {
		return "", constants.ErrBindingNotFound
	}
	return string(etcdRes.Kvs[0].Value), nil
}

//session关闭（连接断开）则移除etcd上的uid和前端服务器id的绑定关系
func (b *ETCDBindingStorage) setupOnSessionCloseCB() {
	session.OnSessionClose(func(s *session.Session) {
		if s.UID() != "" {
			err := b.removeBinding(s.UID())
			if err != nil {
				logger.Log.Errorf("error removing binding info from storage: %v", err)
			}
		}
	})
}

//session bind之后将uid和前端服务器的id放入etcd存储
func (b *ETCDBindingStorage) setupOnAfterSessionBindCB() {
	session.OnAfterSessionBind(func(ctx context.Context, s *session.Session) error {
		return b.PutBinding(s.UID())
	})
}

// watchLeaseChan 监听租约keepalive返回失败的话重新生成租约
func (b *ETCDBindingStorage) watchLeaseChan(c <-chan *clientv3.LeaseKeepAliveResponse) {
	for {
		select {
		case <-b.stopChan:
			return
		case kaRes := <-c:
			if kaRes == nil {
				logger.Log.Warn("[binding storage] sd: error renewing etcd lease, rebootstrapping")
				for {
					err := b.bootstrapLease()
					if err != nil {
						logger.Log.Warn("[binding storage] sd: error rebootstrapping lease, will retry in 5 seconds")
						time.Sleep(5 * time.Second)
						continue
					} else {
						return
					}
				}
			}
		}
	}
}

func (b *ETCDBindingStorage) bootstrapLease() error {
	// 创建租约 带ttl
	l, err := b.cli.Grant(context.TODO(), int64(b.leaseTTL.Seconds()))
	if err != nil {
		return err
	}
	b.leaseID = l.ID
	logger.Log.Debugf("[binding storage] sd: got leaseID: %x", l.ID)
	// this will keep alive forever, when channel c is closed
	// it means we probably have to rebootstrap the lease
	//keep alive
	c, err := b.cli.KeepAlive(context.TODO(), b.leaseID)
	if err != nil {
		return err
	}
	// need to receive here as per etcd docs
	<-c
	//监听keep alive的返回
	go b.watchLeaseChan(c)
	return nil
}

// Init starts the binding storage module
func (b *ETCDBindingStorage) Init() error {
	//连接etcd
	var cli *clientv3.Client
	var err error
	if b.cli == nil {
		cli, err = clientv3.New(clientv3.Config{
			Endpoints:   b.etcdEndpoints,
			DialTimeout: b.etcdDialTimeout,
		})
		if err != nil {
			return err
		}
		b.cli = cli
	}
	// 创建kv存取
	b.cli.KV = namespace.NewKV(b.cli.KV, b.etcdPrefix)
	// 创建租约keepalive
	err = b.bootstrapLease()
	if err != nil {
		return err
	}

	//如果是前端服务器则维护sessiond的生命周期
	if b.thisServer.Frontend {
		b.setupOnSessionCloseCB()
		b.setupOnAfterSessionBindCB()
	}

	return nil
}

// Shutdown executes on shutdown and will clean etcd
func (b *ETCDBindingStorage) Shutdown() error {
	close(b.stopChan)
	return b.cli.Close()
}
