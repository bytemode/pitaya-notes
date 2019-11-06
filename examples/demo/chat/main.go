package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"strings"

	"github.com/spf13/viper"
	"github.com/topfreegames/pitaya"
	"github.com/topfreegames/pitaya/acceptor"
	"github.com/topfreegames/pitaya/component"
	"github.com/topfreegames/pitaya/config"
	"github.com/topfreegames/pitaya/groups"
	"github.com/topfreegames/pitaya/logger"
	"github.com/topfreegames/pitaya/serialize/json"
	"github.com/topfreegames/pitaya/timer"
)

type (
	// Room represents a component that contains a bundle of room related handler
	// like Join/Message
	Room struct {
		component.Base
		timer *timer.Timer
	}

	// UserMessage represents a message that user sent
	UserMessage struct {
		Name    string `json:"name"`
		Content string `json:"content"`
	}

	// NewUser message will be received when new user join room
	NewUser struct {
		Content string `json:"content"`
	}

	// AllMembers contains all members uid
	AllMembers struct {
		Members []string `json:"members"`
	}

	// JoinResponse represents the result of joining room
	JoinResponse struct {
		Code   int    `json:"code"`
		Result string `json:"result"`
	}
)

// NewRoom returns a Handler Base implementation
func NewRoom() *Room {
	return &Room{}
}

// AfterInit component lifetime callback
func (r *Room) AfterInit() {
	r.timer = pitaya.NewTimer(time.Minute, func() {
		count, err := pitaya.GroupCountMembers(context.Background(), "room")
		logger.Log.Debugf("UserCount: Time=> %s, Count=> %d, Error=> %q", time.Now().String(), count, err)
	})
}

// Join room
func (r *Room) Join(ctx context.Context, msg []byte) (*JoinResponse, error) {
	s := pitaya.GetSessionFromCtx(ctx)
	fakeUID := s.ID()                              // just use s.ID as uid !!!
	err := s.Bind(ctx, strconv.Itoa(int(fakeUID))) // binding session uid

	if err != nil {
		return nil, pitaya.Error(err, "RH-000", map[string]string{"failed": "bind"})
	}

	uids, err := pitaya.GroupMembers(ctx, "room")
	if err != nil {
		return nil, err
	}
	s.Push("onMembers", &AllMembers{Members: uids})
	// notify others
	pitaya.GroupBroadcast(ctx, "chat", "room", "onNewUser", &NewUser{Content: fmt.Sprintf("New user: %s", s.UID())})
	// new user join group
	pitaya.GroupAddMember(ctx, "room", s.UID()) // add session to group

	// on session close, remove it from group
	s.OnClose(func() {
		pitaya.GroupRemoveMember(ctx, "room", s.UID())
	})

	return &JoinResponse{Result: "success"}, nil
}

// Message sync last message to all members
func (r *Room) Message(ctx context.Context, msg *UserMessage) {
	err := pitaya.GroupBroadcast(ctx, "chat", "room", "onMessage", msg)
	if err != nil {
		fmt.Println("error broadcasting message", err)
	}
}

func main() {
	defer pitaya.Shutdown()

	//使用Json Serializer
	s := json.NewSerializer()
	conf := configApp()
	//消息序列化
	pitaya.SetSerializer(s)
	//创建一个基于内存的GroupService进行用户分组历
	gsi := groups.NewMemoryGroupService(config.NewConfig(conf))
	pitaya.InitGroups(gsi)
	//创建一个Group
	err := pitaya.GroupCreate(context.Background(), "room")
	if err != nil {
		panic(err)
	}

	// rewrite component and handler name 创建一个组件用来handler消息
	room := NewRoom()

	//组件注册 添加组件到本地的组件列表中 组件在启动之后会注册到handlerService中
	pitaya.Register(room,
		component.WithName("room"),
		component.WithNameFunc(strings.ToLower),
	)

	log.SetFlags(log.LstdFlags | log.Llongfile)

	//启动web服务器
	http.Handle("/web/", http.StripPrefix("/web/", http.FileServer(http.Dir("web"))))
	go http.ListenAndServe(":3251", nil)

	//创建websocket 的acceptor放入到pitaya中
	ws := acceptor.NewWSAcceptor(":3250")
	pitaya.AddAcceptor(ws)

	//是前端服务器  服务器类型 chat  集群模式 元数据为{}  vipper配置为conf
	pitaya.Configure(true, "chat", pitaya.Cluster, map[string]string{}, conf)

	//启动服务器
	pitaya.Start()
}

func configApp() *viper.Viper {
	conf := viper.New()
	conf.SetEnvPrefix("chat")                                 // allows using env vars in the CHAT_PITAYA_ format
	conf.SetDefault("pitaya.buffer.handler.localprocess", 15) //acceptor上管道上传来消息写入待处理管道的大小 接收客户端消息的大小
	conf.Set("pitaya.heartbeat.interval", "15s")              //心跳间隔
	conf.Set("pitaya.buffer.agent.messages", 32)              //agent上write消息的chan大小 发送个client的消息chan的大小
	conf.Set("pitaya.handler.messages.compression", false)    //消息不压缩
	return conf
}
