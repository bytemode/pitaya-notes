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
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/namespace"
	"github.com/topfreegames/pitaya/config"
	"github.com/topfreegames/pitaya/constants"
	"github.com/topfreegames/pitaya/logger"
	"github.com/topfreegames/pitaya/util"
)

//ETCD实现服务发现的核心思想： key存储、 租约、 key监听
// 本地使用一个结构Server保存服务的信息 id type
// 将id和type组装成key server进行Json序列化
// 连接ETCD生成etcd client
// 生成一个租约设置租约时间并且对租约keepalive，同时对租约续租响应通道进行监听(如果keepalive失败则重新一遍本部及下一步的流程)
// 同时注册本地server到ETCD授权租约之后同步ETCD上的server到本地
// 开启定时器定期同步ETCD上服务器信息（get key）
// 使用key监听的方式 监听ETCD上传来的Server的添加或者删除用来更新本地的信息

//server想etcd注册一个key(type/id)写入自身的信息，同时开启定时器获取ETCD上的server同步到本地，
//监听ETCD上指定前缀的key用来监听ETCD上的服务端的变化同时同步到本地

type etcdServiceDiscovery struct {
	cli                  *clientv3.Client              //etcd v3客户端
	config               *config.Config                //viper配置
	syncServersInterval  time.Duration                 //servers 同步时间间隔
	heartbeatTTL         time.Duration                 //心跳间隔
	logHeartbeat         bool                          //是否日志心跳
	lastHeartbeatTime    time.Time                     //上次心跳
	leaseID              clientv3.LeaseID              //租约id
	mapByTypeLock        sync.RWMutex                  //读写所
	serverMapByType      map[string]map[string]*Server //类型 server映射
	serverMapByID        sync.Map                      //同步的map
	etcdEndpoints        []string                      //etcd 节点
	etcdPrefix           string
	etcdDialTimeout      time.Duration
	running              bool
	server               *Server //本机服务
	stopChan             chan bool
	stopLeaseChan        chan bool
	lastSyncTime         time.Time     //上次同步 事件
	listeners            []SDListener  //服务发现监听
	revokeTimeout        time.Duration //撤销超时
	grantLeaseTimeout    time.Duration //设置租约时间超时
	grantLeaseMaxRetries int
	grantLeaseInterval   time.Duration
	shutdownDelay        time.Duration
	appDieChan           chan bool
}

// NewEtcdServiceDiscovery ctor
func NewEtcdServiceDiscovery(
	config *config.Config,
	server *Server,
	appDieChan chan bool,
	cli ...*clientv3.Client,
) (ServiceDiscovery, error) {
	var client *clientv3.Client
	if len(cli) > 0 {
		client = cli[0]
	}
	sd := &etcdServiceDiscovery{
		config:          config,
		running:         false,
		server:          server,
		serverMapByType: make(map[string]map[string]*Server),
		listeners:       make([]SDListener, 0),
		stopChan:        make(chan bool),
		stopLeaseChan:   make(chan bool),
		appDieChan:      appDieChan,
		cli:             client,
	}

	sd.configure()

	return sd, nil
}

// etcdServiceDiscovery 设置服务发现的配置
func (sd *etcdServiceDiscovery) configure() {
	sd.etcdEndpoints = sd.config.GetStringSlice("pitaya.cluster.sd.etcd.endpoints")                  //逗号分隔的etcd端点列表
	sd.etcdDialTimeout = sd.config.GetDuration("pitaya.cluster.sd.etcd.dialtimeout")                 //拨号超时值传递给服务发现etcd客户端
	sd.etcdPrefix = sd.config.GetString("pitaya.cluster.sd.etcd.prefix")                             //避免不同的pitaya冲突 服务器必须具有相同的前缀才能相互看到
	sd.heartbeatTTL = sd.config.GetDuration("pitaya.cluster.sd.etcd.heartbeat.ttl")                  //etcd租约的心跳间隔
	sd.logHeartbeat = sd.config.GetBool("pitaya.cluster.sd.etcd.heartbeat.log")                      //是否应在调试模式下记录etcd心跳
	sd.syncServersInterval = sd.config.GetDuration("pitaya.cluster.sd.etcd.syncservers.interval")    //服务发现模块执行的服务器同步之间的间隔
	sd.revokeTimeout = sd.config.GetDuration("pitaya.cluster.sd.etcd.revoke.timeout")                //etcd的撤销功能超时
	sd.grantLeaseTimeout = sd.config.GetDuration("pitaya.cluster.sd.etcd.grantlease.timeout")        //etcd租期超时
	sd.grantLeaseMaxRetries = sd.config.GetInt("pitaya.cluster.sd.etcd.grantlease.maxretries")       //etcd授予租约的最大尝试次数
	sd.grantLeaseInterval = sd.config.GetDuration("pitaya.cluster.sd.etcd.grantlease.retryinterval") //每次授予租约尝试之间的间隔
	sd.shutdownDelay = sd.config.GetDuration("pitaya.cluster.sd.etcd.shutdown.delay")                //从服务发现注销后等待关闭的时间
}

// watchLeaseChan 监听续租的相应管道LeaseKeepAliveResponse 会收到续租相应的事件
func (sd *etcdServiceDiscovery) watchLeaseChan(c <-chan *clientv3.LeaseKeepAliveResponse) {
	failedGrantLeaseAttempts := 0
	for {
		select {
		case <-sd.stopChan: //服务发现停止
			return
		case <-sd.stopLeaseChan: //是否停止续住监听
			return
		case leaseKeepAliveResponse := <-c: //从续租响应管道获取到通知事件
			if leaseKeepAliveResponse != nil {
				if sd.logHeartbeat {
					logger.Log.Debugf("sd: etcd lease %x renewed", leaseKeepAliveResponse.ID)
				}
				failedGrantLeaseAttempts = 0
				continue
			}
			logger.Log.Warn("sd: error renewing etcd lease, reconfiguring")
			for {
				err := sd.renewLease()
				if err != nil {
					failedGrantLeaseAttempts = failedGrantLeaseAttempts + 1
					if err == constants.ErrEtcdGrantLeaseTimeout {
						logger.Log.Warn("sd: timed out trying to grant etcd lease")
						if sd.appDieChan != nil {
							sd.appDieChan <- true
						}
						return
					}
					if failedGrantLeaseAttempts >= sd.grantLeaseMaxRetries {
						logger.Log.Warn("sd: exceeded max attempts to renew etcd lease")
						if sd.appDieChan != nil {
							sd.appDieChan <- true
						}
						return
					}
					logger.Log.Warnf("sd: error granting etcd lease, will retry in %d seconds", uint64(sd.grantLeaseInterval.Seconds()))
					time.Sleep(sd.grantLeaseInterval)
					continue
				}
				return
			}
		}
	}
}

// renewLease reestablishes connection with etcd 创新生成租约重新启动后服务
func (sd *etcdServiceDiscovery) renewLease() error {
	c := make(chan error)
	go func() {
		defer close(c)
		logger.Log.Infof("waiting for etcd lease")
		err := sd.grantLease()
		if err != nil {
			c <- err
			return
		}
		err = sd.bootstrapServer(sd.server)
		c <- err
	}()
	select {
	case err := <-c:
		return err
	case <-time.After(sd.grantLeaseTimeout):
		return constants.ErrEtcdGrantLeaseTimeout
	}
}

// grantLease创建租约并且续租同时监听续租的相应管道
func (sd *etcdServiceDiscovery) grantLease() error {
	// 创建一个租约到期时间即心跳时间
	l, err := sd.cli.Grant(context.TODO(), int64(sd.heartbeatTTL.Seconds()))
	if err != nil {
		return err
	}
	//记录租约id
	sd.leaseID = l.ID
	logger.Log.Debugf("sd: got leaseID: %x", l.ID)
	// this will keep alive forever, when channel c is closed
	// it means we probably have to rebootstrap the lease
	//租约定期续租保持alive 返回租约续租相应管道
	c, err := sd.cli.KeepAlive(context.TODO(), sd.leaseID)
	if err != nil {
		return err
	}
	// need to receive here as per etcd docs
	//读取管道的值
	<-c
	go sd.watchLeaseChan(c)
	return nil
}

//服务注册 将Server的type和id组装成key 序列化信息做为value将 kv写入etcd
func (sd *etcdServiceDiscovery) addServerIntoEtcd(server *Server) error {
	_, err := sd.cli.Put(
		context.TODO(),
		getKey(server.ID, server.Type), //servers/serverType/serverID 组装key
		server.AsJSONString(),          //server的信息序列化为json数据
		clientv3.WithLease(sd.leaseID), //给key授权租约
	)
	return err
}

// bootstrapServer 启动服务
func (sd *etcdServiceDiscovery) bootstrapServer(server *Server) error {
	//注册服务 将服务信息作为kv写入etcd同时授权租约
	if err := sd.addServerIntoEtcd(server); err != nil {
		return err
	}

	//湖区etcd上的注册的服务器信息对本地保存的服务器信息进行同步
	sd.SyncServers()
	return nil
}

// AddListener adds a listener to etcd service discovery
func (sd *etcdServiceDiscovery) AddListener(listener SDListener) {
	sd.listeners = append(sd.listeners, listener)
}

// AfterInit executes after Init
func (sd *etcdServiceDiscovery) AfterInit() {
}

// notifyListeners 添加删除服务器时进行监听通知
func (sd *etcdServiceDiscovery) notifyListeners(act Action, sv *Server) {
	for _, l := range sd.listeners {
		if act == DEL {
			l.RemoveServer(sv)
		} else if act == ADD {
			l.AddServer(sv)
		}
	}
}

// 加锁执行f
func (sd *etcdServiceDiscovery) writeLockScope(f func()) {
	sd.mapByTypeLock.Lock()
	defer sd.mapByTypeLock.Unlock()
	f()
}

//根据id删除本地的server信息记录
func (sd *etcdServiceDiscovery) deleteServer(serverID string) {
	if actual, ok := sd.serverMapByID.Load(serverID); ok {
		sv := actual.(*Server)
		sd.serverMapByID.Delete(sv.ID)
		sd.writeLockScope(func() {
			if svMap, ok := sd.serverMapByType[sv.Type]; ok {
				delete(svMap, sv.ID)
			}
		})
		sd.notifyListeners(DEL, sv)
	}
}

func (sd *etcdServiceDiscovery) deleteLocalInvalidServers(actualServers []string) {
	sd.serverMapByID.Range(func(key interface{}, value interface{}) bool {
		k := key.(string)
		if !util.SliceContainsString(actualServers, k) {
			logger.Log.Warnf("deleting invalid local server %s", k)
			sd.deleteServer(k)
		}
		return true
	})
}

// getKey 类型和id组装成key servers/serverType/serverID
func getKey(serverID, serverType string) string {
	return fmt.Sprintf("servers/%s/%s", serverType, serverID)
}

// getServerFromEtcd 查询etcd上的server序列化信息
func (sd *etcdServiceDiscovery) getServerFromEtcd(serverType, serverID string) (*Server, error) {
	svKey := getKey(serverID, serverType)
	svEInfo, err := sd.cli.Get(context.TODO(), svKey)
	if err != nil {
		return nil, fmt.Errorf("error getting server: %s from etcd, error: %s", svKey, err.Error())
	}
	if len(svEInfo.Kvs) == 0 {
		return nil, fmt.Errorf("didn't found server: %s in etcd", svKey)
	}
	return parseServer(svEInfo.Kvs[0].Value)
}

// GetServersByType returns a slice with all the servers of a certain type
func (sd *etcdServiceDiscovery) GetServersByType(serverType string) (map[string]*Server, error) {
	sd.mapByTypeLock.RLock()
	defer sd.mapByTypeLock.RUnlock()
	if m, ok := sd.serverMapByType[serverType]; ok && len(m) > 0 {
		// Create a new map to avoid concurrent read and write access to the
		// map, this also prevents accidental changes to the list of servers
		// kept by the service discovery.
		ret := make(map[string]*Server, len(sd.serverMapByType))
		for k, v := range sd.serverMapByType[serverType] {
			ret[k] = v
		}
		return ret, nil
	}
	return nil, constants.ErrNoServersAvailableOfType
}

// GetServers returns a slice with all the servers
func (sd *etcdServiceDiscovery) GetServers() []*Server {
	ret := make([]*Server, 0)
	sd.serverMapByID.Range(func(k, v interface{}) bool {
		ret = append(ret, v.(*Server))
		return true
	})
	return ret
}

func (sd *etcdServiceDiscovery) bootstrap() error {
	//生成租约 监听续租响应管道
	if err := sd.grantLease(); err != nil {
		return err
	}

	//将sd.server信息写入ETCD同时授权租约，之后同步ETCD 上的Server到本地
	if err := sd.bootstrapServer(sd.server); err != nil {
		return err
	}

	return nil
}

// GetServer returns a server given it's id
func (sd *etcdServiceDiscovery) GetServer(id string) (*Server, error) {
	if sv, ok := sd.serverMapByID.Load(id); ok {
		return sv.(*Server), nil
	}
	return nil, constants.ErrNoServerWithID
}

// Init starts the service discovery client 注册模块儿的时候回进行调用
func (sd *etcdServiceDiscovery) Init() error {
	sd.running = true
	var cli *clientv3.Client
	var err error
	if sd.cli == nil { //连接ETCD
		cli, err = clientv3.New(clientv3.Config{
			Endpoints:   sd.etcdEndpoints,
			DialTimeout: sd.etcdDialTimeout,
		})
		if err != nil {
			return err
		}
		sd.cli = cli
	}

	// namespaced etcd :) Package namespace is a clientv3 wrapper that translates all keys to begin with a given prefix.
	sd.cli.KV = namespace.NewKV(sd.cli.KV, sd.etcdPrefix)                // 重写ectd client interfaces 给所有的key 监听 租约加上前缀
	sd.cli.Watcher = namespace.NewWatcher(sd.cli.Watcher, sd.etcdPrefix) //监听
	sd.cli.Lease = namespace.NewLease(sd.cli.Lease, sd.etcdPrefix)       //租约

	//生成租约且设置租约时间并且租约定期续租 同时注册本地server到ETCD授权租约之后同步ETCD上的server到本地
	if err = sd.bootstrap(); err != nil {
		return err
	}

	// update servers 开启定时器定期同步服务器信息
	syncServersTicker := time.NewTicker(sd.syncServersInterval)
	go func() {
		for sd.running {
			select {
			case <-syncServersTicker.C:
				err := sd.SyncServers()
				if err != nil {
					logger.Log.Errorf("error resyncing servers: %s", err.Error())
				}
			case <-sd.stopChan:
				return
			}
		}
	}()

	//使用key监听的方式 监听ETCD上传来的Server的添加或者删除用来更新本地的信息
	go sd.watchEtcdChanges()
	return nil
}

func parseEtcdKey(key string) (string, string, error) {
	splittedServer := strings.Split(key, "/")
	if len(splittedServer) != 3 {
		return "", "", fmt.Errorf("error parsing etcd key %s (server name can't contain /)", key)
	}
	svType := splittedServer[1]
	svID := splittedServer[2]
	return svType, svID, nil
}

func parseServer(value []byte) (*Server, error) {
	var sv *Server
	err := json.Unmarshal(value, &sv)
	if err != nil {
		logger.Log.Warnf("failed to load server %s, error: %s", sv, err.Error())
	}
	return sv, nil
}

func (sd *etcdServiceDiscovery) printServers() {
	sd.mapByTypeLock.RLock()
	defer sd.mapByTypeLock.RUnlock()
	for k, v := range sd.serverMapByType {
		logger.Log.Debugf("type: %s, servers: %+v", k, v)
	}
}

// SyncServers gets all servers from etcd 从etcd获取左右的server对本地保存的server信息进行同步（增删）
func (sd *etcdServiceDiscovery) SyncServers() error {
	//读取servers目录下的所有key
	keys, err := sd.cli.Get(
		context.TODO(),
		"servers/",
		clientv3.WithPrefix(),
		clientv3.WithKeysOnly(),
	)
	if err != nil {
		return err
	}

	// delete invalid servers (local ones that are not in etcd)
	allIds := make([]string, 0)

	// filter servers I need to grab info 获取所有etcd上的server根据key获取server然后添加到本地
	for _, kv := range keys.Kvs {
		svType, svID, err := parseEtcdKey(string(kv.Key)) //根据etcd的key解析出 svType 和svID
		if err != nil {
			logger.Log.Warnf("failed to parse etcd key %s, error: %s", kv.Key, err.Error())
		}
		allIds = append(allIds, svID)
		// TODO is this slow? if so we can paralellize
		if _, ok := sd.serverMapByID.Load(svID); !ok { //查看id是否存在 如果本地存在记录
			logger.Log.Debugf("loading info from missing server: %s/%s", svType, svID)
			//根据svType 和svId组装成key获取Etcd上的Server序列化信息
			sv, err := sd.getServerFromEtcd(svType, svID)
			if err != nil {
				logger.Log.Errorf("error getting server from etcd: %s, error: %s", svID, err.Error())
				continue
			}
			//将sever添加到本地存储server的两种map
			sd.addServer(sv)
		}
	}
	//删除ETCD上没有本地存在的server
	sd.deleteLocalInvalidServers(allIds)

	sd.printServers()
	sd.lastSyncTime = time.Now() //记录下同步的时间
	return nil
}

// BeforeShutdown executes before shutting down and will remove the server from the list
func (sd *etcdServiceDiscovery) BeforeShutdown() {
	sd.revoke()
	time.Sleep(sd.shutdownDelay) // Sleep for a short while to ensure shutdown has propagated
}

// Shutdown executes on shutdown and will clean etcd
func (sd *etcdServiceDiscovery) Shutdown() error {
	sd.running = false
	close(sd.stopChan)
	return nil
}

// revoke prevents Pitaya from crashing when etcd is not available
func (sd *etcdServiceDiscovery) revoke() error {
	close(sd.stopLeaseChan)
	c := make(chan error)
	defer close(c)
	go func() {
		logger.Log.Debug("waiting for etcd revoke")
		_, err := sd.cli.Revoke(context.TODO(), sd.leaseID)
		c <- err
		logger.Log.Debug("finished waiting for etcd revoke")
	}()
	select {
	case err := <-c:
		return err // completed normally
	case <-time.After(sd.revokeTimeout):
		logger.Log.Warn("timed out waiting for etcd revoke")
		return nil // timed out
	}
}

// addServer 将Server存入serverMapByID（map[id]*Server） 和 serverMapByType(map[type]map[id]*Server)
func (sd *etcdServiceDiscovery) addServer(sv *Server) {
	if _, loaded := sd.serverMapByID.LoadOrStore(sv.ID, sv); !loaded {
		//在对serverMapByType进行修改时使用加锁
		sd.writeLockScope(func() {
			mapSvByType, ok := sd.serverMapByType[sv.Type]
			if !ok {
				mapSvByType = make(map[string]*Server)
				sd.serverMapByType[sv.Type] = mapSvByType
			}
			mapSvByType[sv.ID] = sv
		})
		if sv.ID != sd.server.ID {
			//通知监听添加服务器
			sd.notifyListeners(ADD, sv)
		}
	}
}

// 监听所有ETCD上的Server的key
func (sd *etcdServiceDiscovery) watchEtcdChanges() {
	//监视etcd上key前缀为servers的key
	w := sd.cli.Watch(context.Background(), "servers/", clientv3.WithPrefix())

	go func(chn clientv3.WatchChan) {
		for sd.running { //sd在运行
			select {
			case wResp := <-chn: //监听到key有变化
				for _, ev := range wResp.Events {
					switch ev.Type {
					case clientv3.EventTypePut: //添加或者更新事件
						var sv *Server
						var err error
						if sv, err = parseServer(ev.Kv.Value); err != nil {
							logger.Log.Errorf("Failed to parse server from etcd: %v", err)
							continue
						}
						sd.addServer(sv) //添加服务到本地
						logger.Log.Debugf("server %s added", ev.Kv.Key)
						sd.printServers()
					case clientv3.EventTypeDelete: //删除服务事件
						_, svID, err := parseEtcdKey(string(ev.Kv.Key))
						if err != nil {
							logger.Log.Warnf("failed to parse key from etcd: %s", ev.Kv.Key)
							continue
						}
						sd.deleteServer(svID) //删除本地服务
						logger.Log.Debugf("server %s deleted", svID)
						sd.printServers()
					}
				}
			case <-sd.stopChan: //停止
				return
			}
		}
	}(w)
}
