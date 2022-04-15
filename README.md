# Tortoise MQ

一款简易的消息中间件,因为水平有限，它不太快,Tortoise MQ

### TODO

client

- 消费者逻辑(推和拉)

    - 批量拉取消息(已完成)

- 消息tag(可选)

broker

- 消费次数上限(已完成)

- 延时消息(分为16个等级,为了queue不需要进行排序,后续想想自定义时间怎么来实现) (已完成)

- queue文件删除时间之前的数据
    - 删除(已完成)

    - 调整topic的offset(已完成)

- 其他再想想

broker集群