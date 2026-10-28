# 分类与优惠券列表的 MQ 缓存失效

```text
数据库事务提交成功
    → 删除 Redis 缓存
    → 发布失效消息
    → RabbitMQ fanout 广播，每个实例独立队列
    → 消费消息，只删除当前实例的 Caffeine
    → 成功 ACK，失败重试，耗尽后拒绝消息进入死信队列
```

多级缓存仍保留 mysql、redis、caffeine 三种查询模式。无需创建数据库表、触发器或运行迁移脚本。

## 代码结构

业务 MQ 位于 `hmdp-core-service`；共用 JSON、RabbitTemplate、手动 ACK 工厂由 `hmdp-framework` 的 `com.hmdp.framework.mq.config.RabbitConfig` 提供。

```text
com.hmdp.mq
├── config
│   ├── CacheMqProperties          缓存消息配置项
│   └── CacheMqConfig              fanout、实例队列与死信拓扑
├── message
│   └── CacheInvalidationMessage   缓存类型与 shopId
├── producer
│   └── CacheInvalidationPublisher 提交后删 Redis、发送确认与重试
└── consumer
    └── CacheInvalidationConsumer  清理本地缓存、重试与 ACK/拒绝
```

业务通过 `ApplicationEventPublisher.publishEvent()` 发布失效事件。发布者使用 `@TransactionalEventListener(AFTER_COMMIT)`，业务回滚时不执行；无事务时直接执行，调用方必须先完成数据库操作。事件不携带可变的业务实体或旧缓存数据。

商铺分类失效整个列表；优惠券列表按 shopId 失效。查询仍使用原有 `LocalCacheClient`，消费者只调用缓存服务暴露的 `invalidateLocalCache()`。

## 已接入的写入入口

- `ShopTypeServiceImpl.save / updateById / removeById`。
- `VoucherServiceImpl.save / updateById / removeById`：覆盖普通券新增；更新所属商铺时同时失效新旧商铺。
- `VoucherServiceImpl.addSeckillVoucher`：复用 save 发布事件，直到券、秒杀信息和原有库存初始化流程所在的事务提交后才执行失效。
- `SeckillVoucherServiceImpl.updateById`：修改库存或活动时间后失效所属商铺列表。
- `VoucherOrderServiceImpl.createVoucherOrder`：MySQL 扣库存并保存订单后发布事件，事务提交后失效列表。

没有使用通用 SQL 拦截器。新增批量、链式更新、Mapper 直写等入口时，需要在对应事务内显式发布事件；直接修改数据库不会产生通知。例如：

```java
@Transactional
public void updateSomething(...) {
    // 先执行业务数据库写入
    eventPublisher.publishEvent(CacheInvalidationMessage.voucherList(shopId));
    // 分类使用 CacheInvalidationMessage.shopType()
}
```

## 配置

启动 RabbitMQ，按 `application.example.yaml` 设置 `spring.rabbitmq` 连接信息。交换机和队列由应用自动声明。

```yaml
hmdp:
  cache:
    mq:
      enabled: true
      producer-max-attempts: 3  # 包含首次执行；只重试MQ发布
      consumer-max-attempts: 3  # 包含首次执行；在当前投递内重试清理本地缓存
      retry-interval: 500ms
      confirm-timeout: 5s
      queue-expires: 1d
```

默认 fanout exchange 为 `hmdp.cache.invalidate`。每个进程的队列名为 `hmdp.cache.instance.<随机 UUID>`，durable，断线期间保留消息；连续一天未使用后回收。每个实例独立队列，避免竞争消费。重启进程会创建新队列，本地缓存从空开始。

缓存与订单消息共用 `RabbitConfig` 中的 `JacksonJsonMessageConverter` 和 `RabbitTemplate`。消息为 persistent，发布端使用 `convertAndSend()`，通过 `CorrelationData` 检查 Confirm 和 Returns；NACK、超时及无法路由均按配置重试。保留现有缓存发布策略：Redis 删除失败记录错误，但仍广播清理本地缓存，Redis 删除不随发送重试。重试由 Spring 7 的 `RetryTemplate` 完成，发生在当前线程，MQ 故障会增加写接口响应时间。

消费者使用共用 JSON 转换器和 `manualAckFactory`，成功才手动 ACK；清理失败时由自己的 `RetryTemplate` 有限重试，不叠加默认 AUTO 工厂的重试。耗尽后 `basicReject(requeue=false)`，由 DLX 路由到 `hmdp.cache.invalidate.dlq`。非法 JSON 由容器拒绝。消费者退出或连接断开时，未 ACK 消息可以重新投递。没有自动消费死信的监听器，故障修复后可将原消息重新发布到 fanout exchange，广播给所有当前实例。

## 边界

这是按业务提交后广播失效实现的简化方案。数据库提交与消息发送之间仍有进程崩溃窗口；发布重试耗尽时记录错误日志，需要补发。Confirm 保护应用到 Broker 的发布阶段，消费 ACK 保护消费阶段；默认经典队列的 DLX 转发没有端到端发布确认，Broker 故障仍可能丢失死信。

没有保留上一版的 Outbox、数据库触发器、版本令牌、Lua 条件写回或订阅状态切换。因此并发回源/异步刷新期间仍可能回填旧值，断线期间本地缓存也可能暂时保留旧值，依赖后续失效或缓存过期恢复，不能承诺严格一致性。

旧版数据库脚本没有由本次实现执行。如果曾手动安装旧版触发器，应先移除 `cache_shop_type_ai/au/ad`、`cache_voucher_ai/au/ad`、`cache_seckill_ai/au/ad`，再按需清理旧 `tb_cache_outbox`，避免残留触发器继续写入旧表。

## 验证

```shell
mvn "-Dtest=*Test" test
```

单元测试无需连接业务 MySQL、Redis 或 RabbitMQ，覆盖提交后发送、回滚不发送、Redis 删除先于发布、发送失败重试、消费成功 ACK、重试耗尽拒绝、独立队列和 DLX 绑定。
