[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
![version](https://img.shields.io/badge/version-0.1.0-blue.svg?maxAge=2592000)

# Tlink
***T***est ***Flink*** Streaming SQL

本地执行flink streaming SQL快速进行验证，不依赖任何额外组件，帮助业务人员提高SQL开发效率。

##快速上手

tlink.properties内容如下，是运行一个sql需要配置的最小集，默认产生数据的字段为`user,product,amount`

```properties
tlink.streaming.sql.statement=SELECT user, product, SUM(amount) as amounts FROM Orders GROUP BY user, product
tlink.sink.table.fieldNames=user,product,amounts
tlink.sink.table.fieldTypes=LONG,STRING,INT
tlink.sink.table.type=Retract
```

执行下面命令

```shell
java -Dlogback.configurationFile=/opt/tlink/conf/logback.xml -cp  tlink-0.1.0-bundle.jar com.tlink.streaming.sql.Launcher /opt/tlink/conf/tlink.properties
```

在控制台就可以看到类似如下的输出，会显示发送的数据以及sql运行的结果

```shell
sand data:3,foo,2
Result:(true,3,foo,2)
sand data:9,baz,4
Result:(true,9,baz,4)
sand data:9,foo,7
Result:(true,9,foo,7)
sand data:6,baz,6
Result:(true,6,baz,6)
sand data:2,foo,8
Result:(true,2,foo,8)
sand data:3,foo,8
Result:(false,3,foo,2)
```

基于Event time窗口的SQL可以参见下面的配置

```properties
tlink.streaming.sql.statement=SELECT user, TUMBLE_START(rowtime, INTERVAL '5' SECOND) as wStart,  SUM(amount)  FROM Orders GROUP BY TUMBLE(rowtime, INTERVAL '5' SECOND), user

tlink.source.table.fieldNames=user,product,amount,rowtime.rowtime
tlink.source.table.fieldTypes=LONG,STRING,INT,LONG
tlink.source.eventTime.index=3
tlink.streaming.sql.env.timeCharacteristic=EVENT
tlink.sink.table.fieldNames=user,wStart,amounts
tlink.sink.table.fieldTypes=LONG,SQL_TIMESTAMP,INT
tlink.sink.table.type=Append
```

## 特性

目前0.1.0版本支持如下特性

1. 支持基于event time的窗口
2. 支持随机产生数据

## 后续计划

1. 支持指定文件的方式产生数据
2. 支持多流join的sql
3. 支持blink

## 参数说明

| 参数                                     | 默认值               | 含义             |
| ---------------------------------------- | -------------------- | ---------------- |
| tlink.source.table.name                  | 非必填，默认值Orders | 数据源注册的表名 |
| tlink.source.table.fieldNames            |                      |                  |
| tlink.source.table.fieldTypes            |                      |                  |
| tlink.source.eventTime.index             |                      |                  |
| tlink.source.watermark.maxOutOfOrderness |                      |                  |
| tlink.source.producer.mode               |                      |                  |
| tlink.source.producer.total              |                      |                  |
|                                          |                      |                  |
|                                          |                      |                  |
|                                          |                      |                  |
|                                          |                      |                  |
|                                          |                      |                  |
|                                          |                      |                  |
|                                          |                      |                  |
|                                          |                      |                  |
|                                          |                      |                  |
|                                          |                      |                  |
|                                          |                      |                  |
|                                          |                      |                  |

