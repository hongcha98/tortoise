# Tortoise MQ

一款简易的消息中间件,因为水平有限，它不太快,Tortoise MQ

### TODO

client

- 消费者逻辑(推和拉)

    - 批量拉取消息(已完成)

- 消息tag(可选)

broker

- 消费次数上限(已完成)

- 延时消息

- topic queue文件调整

- 根据offset删除已消费信息,可根据消息保留时间来做

- 其他再想想

broker集群