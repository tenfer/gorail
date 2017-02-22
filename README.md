# gorail
gorail目的打造一个可靠、快速、易用的基于mysql binlog的实时推送系统。


##如何使用


##系统组件
1. canal:   负责注册mysql，订阅binlog，并解析binlog事件
2. topic:   整个rail只有单个topic,canal会将binlog数据投递到topic中，每个topic可以注册多个channel，topic和channel是pub/sub关系
3. channel: channel订阅topic,完全copy了topic的message，调用handler消费消息，对超时的消息和出错的消息重试
4. handler：负责消费单条message,需要增加后续实现

##NOTE
项目处于开发阶段

##反馈
如果需求有不满足的地方，可以联系我。
mobile:18017057834
QQ：564387226
email:fansichi@baidu.com



