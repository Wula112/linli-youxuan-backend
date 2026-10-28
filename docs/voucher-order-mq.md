# RabbitMQ 秒杀异步下单

## 流程

```text
POST /voucher-order/seckill/{id}
    → Redis Lua：检查库存与一人一单，原子预扣
    → 发布 VoucherOrderMessage，等待 Confirm 并检查 Returns
    → 返回订单号（代表请求已受理）
    → RabbitMQ Direct Exchange → 共享订单队列
    → 消费者获取订单锁
    → 数据库事务：检查幂等、条件扣库存、保存订单
    → 提交成功后释放锁，容器 AUTO ACK
```

MySQL 库存扣减和订单生成在消费者中执行。保留 Redis 的库存预扣，用于请求入口限量和一人一单。原有 Redis Stream 消费线程、pending-list 轮询、阻塞队列及同步下单历史代码和原有注释均完整注释保留在 `VoucherOrderServiceImpl`，原 Lua（含 XADD）也注释保留在 `seckill.lua`；当前只执行下方新增的 RabbitMQ 逻辑。

## 结构

```text
framework/mq/config/RabbitConfig    基础组件模块：共用JSON、RabbitTemplate及手动ACK工厂
mq/config/CacheMqConfig             缓存广播拓扑
mq/config/VoucherOrderMqConfig      订单Direct拓扑与DLX；生产者/消费者引用其常量
mq/config/VoucherOrderMqProperties  订单发送的确认超时与重试参数
mq/message/VoucherOrderMessage      orderId、userId、voucherId
mq/producer/VoucherOrderPublisher   发布并确认；失败抛出给下单入口
mq/consumer/VoucherOrderConsumer    持锁调用事务服务；成功返回才AUTO ACK
service/impl/VoucherOrderServiceImpl 预扣、失败回退、事务落库
```

上表除通用 `RabbitConfig` 位于 `hmdp-framework`，其余业务代码都在 `hmdp-core-service`。缓存和订单共用一个 RabbitTemplate、一个 JSON 转换器和 Boot 自动管理的连接工厂。订单消费者使用默认 AUTO 工厂；缓存使用手动 ACK 工厂，并保留自己的重试策略。

订单交换机为 `voucher.order.exchange`，routing key 为 `voucher.order.create`，共享队列为 `voucher.order.queue`。多个应用实例竞争消费订单，每条消息由其中一个实例处理。业务异常有限重试，耗尽后拒绝进入 `voucher.order.dlq`；没有自动清空或消费死信的任务。

## 幂等、事务与发送失败

- Redis Lua 用预占 Hash 同时校验一人一单和记录订单号，并原子登记全局 pending ZSet；不再额外维护资格 Set。
- 消费者与发送失败回退共用 `lock:order:{voucherId}:{userId}`，锁覆盖数据库事务的提交。
- 相同订单重复投递先按订单 ID 查询，已落库则直接成功；用户与券的唯一索引兜底一人一单。
- MySQL 使用 `stock > 0` 条件更新，避免负库存；扣减失败或保存订单失败必须抛异常，使整个事务回滚，容器才能正确重试。
- 发布端有限重试，重发始终保留相同业务订单号。不能仅以 Confirm 超时判断消息没有到达。
- 发布失败后，入口先获取同一把订单锁，再查询数据库。订单已提交则返回订单号，保留扣减；未创建则通过 Lua 校验预扣订单号后回退。迟到消息发现预扣已回退或属于新订单，直接跳过。
- 锁被消费端持有、数据库查询失败或无法安全回退时，保留预扣，接口返回带订单号的“结果待确认”，日志记录相同订单号供核对。

`seckill:reservation:{voucherId}` 是 Hash，field=userId，value=orderId，同时承担购买资格和预占归属校验。`seckill:pending` 保存完整的 `voucherId:userId:orderId`，支持按时间对账。订单提交后只清理 pending，保留 Hash；不要在待处理消息存在时删除这些 Redis Key。Redis 记录缺失时需要核对，不能直接释放库存。

## 配置与数据库升级

示例配置已同步到 `application.example.yaml`。Spring Boot 4 使用 `max-retries`，以下是首次消费加两次重试，共三次：

```yaml
spring:
  rabbitmq:
    publisher-confirm-type: correlated
    publisher-returns: true
    template:
      mandatory: true
    listener:
      simple:
        acknowledge-mode: auto
        concurrency: 2
        prefetch: 20
        default-requeue-rejected: false
        retry:
          enabled: true
          max-retries: 2
          initial-interval: 500ms
          multiplier: 2
          max-interval: 2s
hmdp:
  mq:
    voucher-order:
      producer-max-attempts: 3
      retry-interval: 200ms
      confirm-timeout: 3s
```

新建数据库使用 `db/hmdp.sql`，订单表使用 `uk_order_user_voucher_active(user_id, voucher_id, active_order)`，只约束未关闭订单。已有库按顺序升级原唯一索引、支付截止时间以及 `db/voucher-order-timeout.sql`；不要重复执行ALTER，也不要删除重复的历史订单。升级步骤见[自动关单说明](voucher-order-timeout.md)。本次没有修改运行中的业务数据库。

切换前先停止新秒杀请求，让旧版消费者处理完 `stream.orders` 的消息及 pending-list，再停旧版并部署新版。新版不读取旧 Stream，不能在旧消息积压时直接切换。

## 保留的边界

没有引入 Outbox。Redis 预扣与 RabbitMQ 发布仍不是原子操作，Lua同步登记pending，由独立定时任务核查超时记录、清理已提交订单的索引或补发有效预占。预占补发使用原订单号；补发耗尽后在下次超时核查中获取消费者同一把订单锁，重新查库确认未建单且归属匹配后才释放预占。配置上限为0时只核查，不自动释放；Hash缺失等异常仍需人工核查。已建单的超时关闭使用另一条延迟消息链路和数据库扫描，详见[自动关单说明](voucher-order-timeout.md)。经典队列的默认DLX转发也不承诺跨故障不丢消息。预占配置和升级注意事项见[订单对账说明](seckill-order-reconcile.md)。

接口返回订单号表示排队受理，不表示订单已经落库。本人订单查询、模拟支付及超时关闭见[支付说明](voucher-order-payment.md)与[自动关单说明](voucher-order-timeout.md)。活动时间和初始化库存的规则沿用现有实现。

## 验证

```shell
mvn "-Dtest=*Test" test
```

真实联调测试 `VoucherOrderFlowIT` 显式启用，使用专用 MySQL 3308、Redis 6388、RabbitMQ 5678，MySQL 为 root 空密码、RabbitMQ 为 guest/guest。测试随机创建并删除数据库，使用专用测试券 Key 和订单拓扑。必须使用独立测试实例，禁止将这些测试端口映射到业务服务。

```powershell
$env:HMDP_VOUCHER_ORDER_IT = 'true'
mvn "-Dtest=VoucherOrderFlowIT" test
```

覆盖真实 RabbitMQ 落库与重复投递、MySQL 插入失败时库存回滚、业务异常三次消费后进入死信、发送失败回退后旧消息迟到，以及并发 Lua 预扣与重复回退。
