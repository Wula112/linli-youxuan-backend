# RocketMQ 秒杀异步下单（事务消息）

## 流程

```text
POST /voucher-order/seckill/{id}
    → 生成 orderId，发送事务半消息（本地不可见）
    → Broker 落盘半消息，回调本地事务
    → 本地事务执行 Redis Lua：查预占校验一人一单、扣库存、写预占、登记 pending、写事务标记
    → Lua 返回 0：COMMIT_MESSAGE，半消息转为可消费消息
    → Lua 返回 1/2：ROLLBACK_MESSAGE，半消息被丢弃，接口返回库存不足/重复下单
    → 返回订单号（代表请求已受理）
    → RocketMQ 共享消费者组 → 消费者获取订单锁
    → 数据库事务：检查幂等、条件扣库存、保存订单
    → 提交成功后释放锁，返回 CONSUME_SUCCESS
```

MySQL 库存扣减和订单生成在消费者中执行。Redis 预扣保留在本地事务里，用于入口限量和一人一单。原有 Redis Stream 消费线程、pending-list 轮询、阻塞队列及同步下单历史代码和原有注释均完整注释保留在 `VoucherOrderServiceImpl`，原 Lua（含 XADD）也注释保留在 `seckill.lua`；当前只执行下方新增的 RocketMQ 事务消息逻辑。

**为什么用事务消息。** 原实现是“先 Lua 扣减、再发消息”，两步之间可能丢消息。当前实现先发送半消息，在本地事务中执行 Lua，并由 Broker 根据事务标记回查；已知异常时序的限制见下方“保留的边界”。

## 结构

```text
mq/config/RocketMqConstants        主题、标签、消费者组常量
mq/config/RocketMqProperties       地址、发送重试、消费重投与线程数
mq/config/RocketMqConfig           事务生产者与三条消费者的装配
mq/message/VoucherOrderMessage     orderId、userId、voucherId
mq/producer/SeckillTxContext      随半消息传入本地事务，回填 Lua 返回码
mq/producer/SeckillTransactionListener 执行本地事务；Broker 回查时读事务标记
mq/producer/VoucherOrderPublisher  发送事务消息；对账补发普通消息
mq/consumer/VoucherOrderConsumer   持锁调用事务服务；成功才返回 CONSUME_SUCCESS
repository/redis/SeckillReservation 预扣、回退、pending 清理与事务标记
service/impl/VoucherOrderServiceImpl 下单入口、失败回退、事务落库
```

订单消息与延迟关单消息共用主题 `hmdp-voucher-order`，用标签 `create` / `timeout` 区分，各自一个消费者组；缓存失效使用主题 `hmdp-cache-invalidation`。订单消费者组 `hmdp-voucher-order-consumer` 是共享的，多个应用实例竞争消费。

## 事务回查

本地事务抛异常时返回 `UNKNOW`，半消息留在 Broker，由 Broker 发起回查：

- 回查读到 `seckill:txn:{orderId}` 存在 → `COMMIT_MESSAGE`，消息最终可消费；
- 标记不存在 → `ROLLBACK_MESSAGE`，半消息丢弃，不会产生订单；
- 回查自身失败 → 继续 `UNKNOW`，等待下一次回查。

标记由 `seckill.lua` 与扣减同脚本写入，TTL 固定为 1 小时。需要结合 Broker 回查参数和故障恢复时间核对保留期；标记过期或丢失可能使回查结果不准确。发布端若在提交确认阶段失败，当前仍以本地事务返回码为准。

## 幂等、事务与失败处理

- Redis Lua 用预占 Hash 同时校验一人一单和记录订单号，并原子登记全局 pending ZSet；不再额外维护资格 Set。
- 消费者与失败回退共用 `lock:order:{voucherId}:{userId}`，锁覆盖数据库事务的提交。
- 相同订单重复投递先按订单 ID 查询，已落库则直接成功；用户与券的唯一索引兜底一人一单。
- MySQL 使用 `stock > 0` 条件更新，避免负库存；扣减失败或保存订单失败必须抛异常，使整个事务回滚，消费者才能返回 `RECONSUME_LATER`。
- 消费失败由 Broker 按 `max-reconsume-times` 重投，耗尽后进入 `%DLQ%hmdp-voucher-order-consumer`；没有自动消费死信的任务。
- 本地事务返回 `UNKNOWN` 时，入口先获取同一把订单锁再查询数据库：订单已提交则返回订单号，保留扣减；未创建则通过 Lua 校验预扣归属后回退。迟到消息发现预占已回退或属于新订单，直接跳过。
- 锁被消费端持有、数据库查询失败或无法安全回退时，保留预扣，接口返回带订单号的“结果待确认”，日志记录相同订单号供核对。

`seckill:reservation:{voucherId}` 是 Hash，field=userId，value=orderId，同时承担购买资格和预占归属校验。`seckill:pending` 保存完整的 `voucherId:userId:orderId`，支持按时间对账。订单提交后只清理 pending，保留 Hash；不要在待处理消息存在时删除这些 Redis Key。Redis 记录缺失时需要核对，不能直接释放库存。

## 配置

```yaml
hmdp:
  rocketmq:
    name-server: 127.0.0.1:9876
    producer-group: hmdp-producer
    send-timeout: 3s
    retry-times-when-send-failed: 2
    max-reconsume-times: 2   # 首次消费 + 2 次重投，耗尽进入死信
    consume-threads: 4
    transaction-check-threads: 4
```

使用 5.x 服务端（本仓库本地部署为 `rocketmq-all-5.5.1`）：延迟关单依赖 5.x 定时消息按支付超时精确投递；4.x 服务端会忽略该参数，此时由数据库扫描兜底。主题和消费者组能否自动创建取决于 Broker 配置，启动前需核对。

新建数据库使用 `db/hmdp.sql`，订单表使用 `uk_order_user_voucher_active(user_id, voucher_id, active_order)`，只约束未关闭订单。已有库按顺序升级原唯一索引、支付截止时间以及 `db/voucher-order-timeout.sql`；不要重复执行ALTER，也不要删除重复的历史订单。升级步骤见[自动关单说明](voucher-order-timeout.md)。本次没有修改运行中的业务数据库。

切换前先停止新秒杀请求，让旧版消费者处理完积压消息，再停旧版并部署新版。新版不读取旧 Stream，不能在旧消息积压时直接切换。

## 保留的边界

当前实现还有待处理的事务时序：入口在结果未知时可能回退预占，但事务标记仍存在；回查发现标记缺失时也没有阻止迟到的预扣。因此不能仅凭现有半消息实现认定 Redis 预扣与消息提交始终一致。订单主题同时用于事务消息、普通补发和延迟消息，在启用消息类型校验的 Broker 上也需要核对兼容性。本次目录重组不改变上述行为。

Redis 与 MySQL 不是同一个事务：消费者落库可能失败并重投，重投耗尽后订单缺失，此时由定时对账按 pending 记录补发或释放预占。预占补发使用原订单号；补发耗尽后在下次超时核查中获取消费者同一把订单锁，重新查库确认未建单且归属匹配后才释放预占。配置上限为0时只核查，不自动释放；Hash缺失等异常仍需人工核查。已建单的超时关闭使用延迟消息和数据库扫描，详见[自动关单说明](voucher-order-timeout.md)。预占配置和升级注意事项见[订单对账说明](seckill-order-reconcile.md)。

接口返回订单号表示排队受理，不表示订单已经落库。本人订单查询、模拟支付及超时关闭见[支付说明](voucher-order-payment.md)与[自动关单说明](voucher-order-timeout.md)。活动时间和初始化库存的规则沿用现有实现。

## 验证

```shell
mvn "-Dtest=*Test" test
```

单元测试不连接 Broker：`SeckillTransactionListenerTest` 覆盖提交/回滚/回查三分支，`VoucherOrderPublisherTest` 覆盖半消息发送与结果透传，`SeckillReservationTest` 校验脚本参数与事务标记，`VoucherOrderConsumerTest` 覆盖持锁、重投与毒消息。

原 `VoucherOrderFlowIT` 依赖真实 RabbitMQ 拓扑与队列断言，已随链路迁移删除；新的端到端联调需要可用的 RocketMQ 5.x 集群、MySQL 与 Redis，暂未纳入自动化测试。
