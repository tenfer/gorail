# gorail
gorail目的打造一个可靠、快速、易用的基于mysql binlog的实时推送系统。
 
## quick start(快速使用)
1. 确保订阅的mysql的binlog format是ROW，不是的话，按下面的方式修改 
    ```sh
    * 查看binlog格式
   
    > mysql  -uroot -p123456 -h127.0.0.1 -P3306 test
    mysql> show variables like 'binlog_format%';
    +---------------+-------+
    | Variable_name | Value |
    +---------------+-------+
    | binlog_format | ROW   |
    +---------------+-------+
    1 row in set, 1 warning (0.00 sec)
    
    * 动态修改binlog format为ROW   
    mysql> SET GLOBAL binlog_format = 'ROW';  
    
    * 永久修改，需要更改my.ini配置, 修改binlog_format="ROW",重启mysql服务
    ```
1. 新建test表
    ```sh
    USE test;
    CREATE TABLE IF NOT EXISTS `test` (
    `id` bigint(10) NOT NULL AUTO_INCREMENT,
    `name` varchar(20) NOT NULL,
    `age` int(4) default 0,
    PRIMARY KEY (`id`)
    ) ENGINE=InnoDB  DEFAULT CHARSET=utf8;

    ```
1. [下载最新版本](https://github.com/tenfer/gorail/archive/1.1.0.tar.gz) 
1. 解压，tar -zxvf gorail-{version}.tar.gz &&  cp -r gorail /home/{myuser} && cd /home/{myuser}/gorail
1. vim etc/rail.toml,默认Addr=127.0.0.1:3306 用户名=root 密码=123456修改，不是请自行修改
1. 运行服务,bin/rail -config etc/rail.toml
1. 新增channel，用于消费消息
    ```sh
    curl -X POST   http://127.0.0.1:2060/channel/add -d 'name=test&ctype=http&httpUrl=http://127.0.0.1:2060/test&filter={"schemas":["test"],"tables":["test"],"actions":["*"],"expression":"age > 0"}'
    ```
1. 往 test表插一条记录 
    ```sh
    INSERT INTO test(name,age) VALUES('John', 20);
    ```
1. cat log/rail.log |grep "gorail_test" 是不是打印了新增的记录?(具体见：**推送记录格式说明**),这是httpUrl配置的下游接口打印的请求参数，可以用作测试使用。生产中你需要实现自己的下游接口用于自己的业务

## 系统组件
1. canal:   负责注册mysql，订阅binlog，并解析binlog事件
2. topic:  canal会将binlog数据投递到topic中，每个topic可以注册多个channel，支持通过http动态注册, [API文档](doc/api.md)
3. channel: topic和channel是pub/sub关系, 每个channel负责调用下游http接口推送消息，出错的消息会重试，保证数据最终一致性，支持多种重试策略
4. 监控: 输出prometheus直接使用的metrics, 命名空间是gorail,监控的信息包括：1 QPS 2 队列长度 3 接口平均耗时等等 

## 推送记录格式说明
```sh
{
    "id":"07cfa6a5ce400009",  //消息唯一ID
    "timestamp":1492154350659966500, //消息生成时间,严格时间顺序，单位纳秒
    "attempts":8, //消息重试次数,同一条消息推送失败会多次重试(正常重试次数为1)
    "action":"update",//操作 insert|update|delete
    "schema":"test", //数据库名
    "table":"test", //表名
    "rows":[
        {
            "id":1,  //表字段
            "name":"John",
            "age":1
        }
    ],
    "raw_rows":[], //更新语句，保留更新前的数据
    "primary_keys":[[1]] //主键, 2维数组, 当action是insert或者delete，单一主键[[1]],联合主键[[1,"John",20]];当action=update [[1],[1]],联合主键[[1,"John",20],[1,"John",21]]
}
```

## 过滤器规则
类C表达式，必须是条件表达式
比如： (status == 1 || status == 2 ) && age > 10 

[详细规则](https://github.com/Knetic/govaluate/blob/master/MANUAL.md)

## 最佳实践
* gorail为了达到最终一致性，失败消息会做失败重试,所以下游接口必须支持**幂等性**(可重入)。
* 推荐使用promethues定时采集metrics,地址[http://127.0.0.1:2061/metrics](http://127.0.0.1:2061/metrics),其中gorail_channel_queue_size显示队列长度，对实时性要求高的系统需要密切关注这个指标。如果队列堵塞：
    * 优化下游接口性能
    * 提高channel的并发数
* gorail原理是伪装成mysql从库，要解决单点问题，可以增加gorail实例，当然下游会收到重复请求，根据自己的业务做权衡吧。

## metrics 字段说明
field | 描述
------ | ------
gorail_avg_cost | 平均耗时, 两个维度:name和status,其中 name标识一个操作，比如channel推送操作，命名格式为：downstream_{channelName};status 取值OK或者ERR，分别代表操作成功或者异常
gorail_qps | 每秒请求数,两个维度意义同上
gorail_topic_queue_size | 队列长度，可以用来判断分发到channel拥塞程度
gorail_channel_queue_size | channel自己的队列长度,只有一个维度：name，命名格式为：{channelName}, 标识队列拥塞程度
gorail_channel_retry_queue_size | channel自己的重试队列长度,只有一个维度：name，命名格式为：{channelName}, 标识重试队列拥塞程度

## 应用
项目已经在【趣头条】产品，用于文章入库elastic search,文章分词等应用

## 反馈
如有需求不满足的地方，直接联系我，当然欢迎Pull Request.
QQ：564387226
email:fansichi@qq.com



