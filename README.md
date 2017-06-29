# gorail
gorail目的打造一个可靠、快速、易用的基于mysql binlog的实时推送系统。
 
## Quick start(快速使用)
1. 确保订阅的mysql的binlog format是ROW，不是的话，按下面的方式修改 
 * 查看binlog格式
```sh
> mysql  -uroot -p123456 -h127.0.0.1 -P3306 test
mysql> show variables like 'binlog_format%';
+---------------+-------+
| Variable_name | Value |
+---------------+-------+
| binlog_format | ROW   |
+---------------+-------+
1 row in set, 1 warning (0.00 sec)
```
 * 动态修改binlog format为ROW
 ```sh
 mysql> SET GLOBAL binlog_format = 'ROW';  
 ```
 * 永久修改，需要更改my.ini配置, 修改binlog_format="ROW",重启mysql服务

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

1. 修改配置文件，默认配置文件 etc/rail.toml，默认Addr=127.0.0.1:3306 用户名=root 密码=123456修改，不是请自行修改
1. 下载relases版本(https://github.com/tenfer/gorail/archive/1.0.0.tar.gz) 
1. cd gorail && bin/rail 
1. 新增channel
```sh
curl -X POST   http://127.0.0.1:2060/channel/add -d 'name=test&ctype=http&httpUrl=http://127.0.0.1:2060/test&filter={"schemas":["test"],"tables":["test"],"actions":["*"],"expression":"age > 0"}'
```
1. 往 test表插一条记录 
```sh
INSERT INTO test(name,age) VALUES('John', 20);
```
1. 查看 log/rail.log 是不是打印了新增的记录？ ok，下游接口>http://127.0.0.1:2060/test，只是一个方便为了测试的接口，仅仅是输出请求参数，你可以实现自己的逻辑，比如写缓存、搜索引擎、另外的mysql集群等等


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

## NOTE
项目已经在【趣头条】推荐服务中使用

## 反馈
如有需求不满足的地方，直接联系我，当然欢迎Pull Request.
QQ：564387226
email:fansichi@qq.com



