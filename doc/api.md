# api document

## /channel/add
添加一个新channel用于消费数据，下游接口使用gorail自带的/test接口，只会把请求参数输出到日志。
添加已经存在的channel会报错

```javascript
curl -X POST   http://127.0.0.1:2060/channel/add -d 'name=test&ctype=http&httpUrl=http://127.0.0.1:2060/test&filter={"schemas":["test"],"tables":["test"],"actions":["*"],"expression":"age > 1 && age < 50"}'
```
field | desc 
------------ | -------------
name | channel name, should be unique
ctype | downstream interface's protocol,Now only support http
httpUrl | the url of downstream interface that you push  
filter | json string, push messages that match the filter conditions;<br>above means pushing these messages that  database is test,table is test, all actions(insert,update,delete) and each row's status equal 1,type equal 2
connectTimeoutMs | default is 1000 ms
readWriteTimeoutMs | default is 2000 ms
retryMaxTimes | default is 50
retryIntervalSec | default is 60 seconds
retryStrategy | 0 or 1. if 0, retry intervals all is equal retryIntervalSec's value, else like 1min,2min,4min,8min  ... etc
concurrentNum | 并发数，default is 1

## /channel/del
删除channel,如果channel记录尚有未消费记录，也会随之情况，请谨慎使用

如果想要更新channel，比如你可能想修改并发数来提高推送能力，请不要**先删除后添加**，推荐**新增配置相同（除了name和改动项不一样）的channel，确认无问题再删除**

```javascript
curl -X POST   http://127.0.0.1:2060/channel/del -d 'name=test'
```

## /channel/get
获取channel的配置信息

```javascript
curl -X POST   http://127.0.0.1:2060/channel/get -d 'name=test'
```

 ## /channel/pause
启动或者暂停channel推送消息,一般用在下游出问题时，待发送的消息会写到channel独有的队列中

```javascript
curl -X POST   http://127.0.0.1:2060/channel/pause -d 'name=test&pause=1'
```   
field | desc 
------------ | -------------
pause | 0 启动 1 暂停

 ## /topic/pause
启动或者暂停topic推送消息,如果暂停，消息会写到队列，不会分发到channel，但binlog日志还是在源源不断读取

```javascript
curl -X POST   http://127.0.0.1:2060/topic/pause -d 'name=topic1&pause=1'
```   
field | desc 
------------ | -------------
pause | 0 启动 1 暂停