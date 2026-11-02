# 分类与优惠券列表的 MQ 缓存失效

```text
数据库事务提交成功
    → 删除 Redis 缓存
    → 发布失效消息到 hmdp-cache-invalidation
    → RocketMQ 广播：每个实例一个消费者组
    → 消费消息，只删除当前实例的 Caffeine
    → 成功 CONSUME_SUCCESS，失败重投，耗尽后进入死信主题
```

多级缓存仍保留 mysql、redis、caffeine 三种查询模式。无需创建数据库表、触发器或运行迁移脚本。

## 代码结构

业务 MQ 位于 `hmdp-core-service` 的 `com.hmdp.mq`，主题常量、客户端装配与消息编解码也在该目录下。

```text
com.hmdp.mq
├── config
│   ├── RocketMqConstants          主题、标签与消费者组
│   ├── RocketMqProperties         客户端配置
│   └── RocketMqConfig             事务生产者与三条消费者
├── support
│   └── MqMessageCodec             独立 JsonMapper 编解码
├── message
│   └── CacheInvalidationMessage   缓存类型与 shopId
├── producer
│   └── CacheInvalidationPublisher 提交后删 Redis、发送消息
└── consumer
    └── CacheInvalidationConsumer  清理本地缓存、失败重投
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

启动 RocketMQ 5.x，按 `application.example.yaml` 设置 `hmdp.rocketmq.name-server`。主题与消费者组由客户端首次使用时创建。

```yaml
hmdp:
  rocketmq:
    name-server: 127.0.0.1:9876
  cache:
    mq:
      enabled: true   # false 时连消费者一起不装配，便于基线压测
```

失效广播使用主题 `hmdp-cache-invalidation`、标签 `invalidate`。每个进程使用独立的消费者组 `hmdp-cache-invalidation-<随机 UUID>`，组内只有自己，等价于原来的实例独立队列：所有实例都会收到每条消息，互不竞争。组名每次启动重新生成，重启后只消费启动之后的消息，本地缓存本来就是空的；旧组不再有心跳后由 Broker 逐步清理。

缓存与订单消息共用 `TransactionMQProducer` 和 `MqMessageCodec`。发布端调用 `send()` 并检查 `SendStatus`，失败重试由客户端的 `retry-times-when-send-failed` 完成。保留现有缓存发布策略：Redis 删除失败记录错误，但仍广播清理本地缓存，Redis 删除不随发送重试。重试发生在当前线程，MQ 故障会增加写接口响应时间。

消费者用同一个编解码器反序列化，成功清理返回 `CONSUME_SUCCESS`；清理失败返回 `RECONSUME_LATER`，按 `max-reconsume-times` 重投，耗尽后进入 `%DLQ%<实例消费者组>`。消息体损坏时记录日志后跳过，不阻塞后续消息。进程重启会换新消费者组，未消费的旧消息随旧组一起失效——本地缓存此时为空，不需要补偿。没有自动消费死信的监听器，故障修复后可查询死信主题并重新发布。

## 边界

这是按业务提交后广播失效实现的简化方案。数据库提交与消息发送之间仍有进程崩溃窗口；发布失败时记录错误日志，需要补发。`SendStatus` 只说明 Broker 已接收消息，消费重投与死信由 Broker 按 `max-reconsume-times` 负责。

没有保留上一版的 Outbox、数据库触发器、版本令牌、Lua 条件写回或订阅状态切换。因此并发回源/异步刷新期间仍可能回填旧值，断线期间本地缓存也可能暂时保留旧值，依赖后续失效或缓存过期恢复，不能承诺严格一致性。

旧版数据库脚本没有由本次实现执行。如果曾手动安装旧版触发器，应先移除 `cache_shop_type_ai/au/ad`、`cache_voucher_ai/au/ad`、`cache_seckill_ai/au/ad`，再按需清理旧 `tb_cache_outbox`，避免残留触发器继续写入旧表。

## 验证

```shell
mvn "-Dtest=*Test" test
```

单元测试无需连接业务 MySQL、Redis 或 RocketMQ，覆盖提交后发送、回滚不发送、Redis 删除先于发布、发送失败不影响已提交事务、消费成功与重投分支，以及非法消息体跳过。
