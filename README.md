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
3. 支持指定文件的方式产生数据

## 后续计划

1. 支持多流join的sql
2. 支持blink planer

## 参数说明

| 参数                                                  | 默认值                                  | 含义                                                         |
| ----------------------------------------------------- | --------------------------------------- | ------------------------------------------------------------ |
| tlink.source.table.name                               | 非必填，默认值Orders                    | 数据源注册的表名                                             |
| tlink.source.table.fieldNames                         | 非必填，默认值user, product, amount     | 数据源字段名                                                 |
| tlink.source.table.fieldTypes                         | 非必填，默认值LONG, STRING, INT         | 数据源字段类型                                               |
| tlink.source.eventTime.index                          | 如果使用event time必填                  | event time字段在所有字段中的位置                             |
| tlink.source.watermark.maxOutOfOrderness              | 如果使用event time必填，默认值10000毫米 | 最大允许延迟时间                                             |
| tlink.source.producer.mode                            | 非必填，默认值random                    | 产生数据的方式，可选值random或者file                         |
| tlink.source.producer.file.path                       | 如果上面参数配置file，必填              | 数据文件绝对路径                                             |
| tlink.source.producer.total                           | 非必填，默认20                          | 随机模式下总共产生的数据量                                   |
| tlink.source.producer.interval.ms                     | 非必填，默认1000毫秒                    | 产生数据的固定时间间隔，如果不配置，采用下面的随机时间间隔   |
| tlink.source.producer.interval.random.startInclusive  | 非必填，默认值1                         | 默认含义为RandomUtils.nextLong(1,5)*1000                     |
| tlink.source.producer.interval.random.endExclusive    | 非必填，默认值5                         | 默认含义为RandomUtils.nextLong(1,5)*1000                     |
| tlink.source.producer.interval.random.factor          | 非必填，默认值1000                      | 默认含义为RandomUtils.nextLong(1,5)*1000                     |
| tlink.source.producer.string.values                   | 非必填，默认值foo, bar, baz             | 字符串字段候选数据集，随机选择一个作为string类型字段的值     |
| tlink.source.producer.long.random.startInclusive      | 非必填，默认值1                         | 默认含义为RandomUtils.nextLong(1,10)*1                       |
| tlink.source.producer.long.random.endExclusive        | 非必填，默认值10                        | 默认含义为RandomUtils.nextLong(1,10)*1                       |
| tlink.source.producer.long.random.factor              | 非必填，默认值1                         | 默认含义为RandomUtils.nextLong(1,10)*1                       |
| tlink.source.producer.int.random.startInclusive       | 非必填，默认值1                         | 默认含义为RandomUtils.nextInt(1,10)*1                        |
| tlink.source.producer.int.random.endExclusive         | 非必填，默认值10                        | 默认含义为RandomUtils.nextInt(1,10)*1                        |
| tlink.source.producer.int.random.factor               | 非必填，默认值1                         | 默认含义为RandomUtils.nextInt(1,10)*1                        |
| tlink.source.producer.timestamp.random.startInclusive | 非必填，默认值1                         | 默认通过RandomUtils.nextLong(1,10)*1000产生一个随机数，如果是偶数当前时间戳减去这个随机数作为event time，如果是奇数当前时间戳加上这个随机数作为event time |
| tlink.source.producer.timestamp.random.endExclusive   | 非必填，默认值10                        | 默认通过RandomUtils.nextLong(1,10)*1000产生一个随机数，如果是偶数当前时间戳减去这个随机数作为event time，如果是奇数当前时间戳加上这个随机数作为event time |
| tlink.source.producer.timestamp.random.factor         | 非必填，默认值1000毫秒                  | 默认通过RandomUtils.nextLong(1,10)*1000产生一个随机数，如果是偶数当前时间戳减去这个随机数作为event time，如果是奇数当前时间戳加上这个随机数作为event time |
| tlink.sink.table.name                                 | 非必填，默认值Output                    | 输出表的名字                                                 |
| tlink.sink.table.fieldNames                           | 必填，无默认值                          | 输出字段                                                     |
| tlink.sink.table.fieldTypes                           | 必填，无默认值                          | 输出字段类型                                                 |
| tlink.sink.table.type                                 | 必填，无默认值                          | 输出表的类型，可选值为Append或者Retract                      |
| tlink.streaming.sql.env.parallelism                   | 非必填，默认值1                         | 并行度                                                       |
| tlink.streaming.sql.env.timeCharacteristic            | 非必填，默认值PROCESSING                | 设置使用那种时间机制，可选值为EVENT或者PROCESSING            |
| tlink.streaming.sql.statement                         | 必填，无默认值                          | 要执行的sql语句                                              |
