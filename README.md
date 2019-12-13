# pitaya-notes
Scalable game server framework with clustering support, code notes
分布式游戏服务器pitaya,代码注释

结合项目使用pitaya的情况，对框架代码进行阅读和注释，加深理解. 同时对项目中遇到的情况进行说明给出解决方法.
目前pitaya因为是国外开源国内无社区，开源速度算是一般，架构ok但是实现上有待改善.

## 架构简述
游戏分为前后端服务，前端服务可以监听客户端连接，后端服务只接受rpc.      
rpc底层实现为nats或者grpc. rpc分为两种sys rpc 和user rpc.sys rpc是服务之间      
转发客户端消息的流程，user rpc是服务器之间用户代码实现的调用.       

组件注册会生成服务，会根据反射将方法变为接收调用的反射信息，调用分析消息handler和rpc两种.       
HandlerService      
将组件Component中的符合Handler的方法遍历处理按照路由名字生成一个map放入HandleServices       
一个组件生成一个Service存储组件本身和组件中handler的反射信息 同时将组件名字和service关联 且记录所有的路由和Handle的反射信息.        

RemoteService       
将组件Component，遍历组件方法取出符合rpc调用的方法封装为Remote 并且存储为method-name:Remote的map.       

消息接收        
app启动对每个acceptor上管道上传来的conn HandlerService创建一个agent然后读取数据成成消息写入消息管道,对管道消息进行处理，本地消息则交由本地服务器处理,  
远程消息则交由remoteService处理.消息中存在servertype如果和本地不同则是remote消息，需要进行一次sys rpc转发调用到符合的service中调用本地方法.         

## 关于RPC
游戏服务器中往往存在这多用开发语言，grpc对各种语言提供和很好的支持，非常的方便.

## 存在问题
1. 消息不能大于2048byte
   推荐同步nano的handler中的接收消息逻辑
2. websocket使用上存在多线程写问题
   推荐使用nano的心跳和发送消息逻辑

