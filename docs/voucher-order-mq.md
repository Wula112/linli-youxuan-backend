# RocketMQ 秒杀异步下单

```text
秒杀请求 → 生成 orderId → 事务半消息
         → Redis Lua 预扣库存、登记预占与 pending、写事务标记
         → Broker 提交消息 → 消费者锁定订单处理记录
         → MySQL 事务：扣库存、创建订单与路由、状态 CREATED
         → 提交后清理 pending，并确认消费

消费失败 → RocketMQ 重试 → 达到上限进入死信
         → 独立死信消费者锁定处理记录并登记 DEAD_LETTER
         → XXL-JOB 对账决定 COMPENSATING → Redis 返库 → COMPENSATED
```

接口返回订单号表示请求已受理，不表示订单已经落库。消费者与对账使用同一条 MySQL 处理记录仲裁，重复消息不会重复建单；迟到死信不会覆盖已建单状态。对账不再补发下单消息。

死信消费者独立订阅 `%DLQ%hmdp-voucher-order-consumer`，使用组 `hmdp-voucher-order-dead-letter-consumer`，从仍保留的最早消息开始。死信成功登记后才确认；数据库访问失败或消息无法解析时返回失败，由 Broker 重试。死信消费者自身重试耗尽后的死信也需要运维监控，无法解析的消息无法凭空生成订单标识。

`hmdp.rocketmq.max-reconsume-times` 控制消费重投次数（默认 2 次）；具体重试间隔由 RocketMQ 消费模式决定。业务处理表不保存失败原因或次数，应用日志记录处理异常。

生产者使用事务半消息。Redis 事务标记 `seckill:txn:{orderId}` 与预扣同脚本写入，回查存在则提交、缺失则回滚，标记保留 1 小时。入口本地事务结果未知时，通过处理记录仲裁：订单已创建则保留库存；否则记录待补偿，由对账任务释放。

Redis、MySQL 和 RocketMQ 不构成单个原子事务。若半消息最终未提交、消息过期或死信登记无法完成，Redis pending 可能没有对应死信；对账只保留并记录该情况，不自动补发或盲目返库。事务标记需要覆盖回查窗口；Redis 和处理终态需要可靠持久化、备份并覆盖重放窗口。

订单主题还承载定时关单消息；启用 Broker 消息类型校验时应核对事务消息和定时消息的主题兼容性。订单处理状态、Redis 凭证、升级与验证见[订单对账说明](seckill-order-reconcile.md)，关单见[自动关单说明](voucher-order-timeout.md)。
