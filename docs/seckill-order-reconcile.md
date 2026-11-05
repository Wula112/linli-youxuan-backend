# 秒杀订单处理与对账

下单消息由 RocketMQ 自行重试；重试耗尽后，独立死信消费者登记处理记录。对账任务不再补发下单消息。任务由 XXL-JOB 的 `seckillOrderReconcile` 调度；未启用 XXL profile 时沿用本地调度。

## 处理记录和状态

`tb_voucher_order_process` 每笔订单一行，字段为 `order_id`（主键）、`user_id`、`voucher_id` 和 `status`。建单、死信登记、对账、入口结果未知时的预占回退，以及超时关单都通过同一记录协调；不另存失败原因或重试次数。

```text
PENDING ──建单成功──────────────────────────> CREATED
   │                                             │
   │ 死信登记                                    │ 到期关单并返还 MySQL 库存
   ▼                                             ▼
DEAD_LETTER ──对账决定释放预占──────────────> COMPENSATING ──Redis 返还成功──> COMPENSATED
   │
   └────迟到消费者先取得处理权且成功建单────> CREATED

PENDING / DEAD_LETTER ──入口结果未知、决定终止──> COMPENSATING
```

建单事务先插入或获取处理记录并锁定，再扣减 MySQL 库存、插入订单与路由、更新为 CREATED；提交后才确认 MQ。死信登记同样锁定处理记录，只有仍为 PENDING 且不存在已提交订单才改成 DEAD_LETTER。迟到的死信不能覆盖 CREATED 或补偿状态。消费者在对账决策前仍可从 DEAD_LETTER 正常建单；对账先改成 COMPENSATING 后，迟到消费者不得建单。

超时关单在同一数据库事务中锁定处理记录、条件关闭未支付订单、返还 MySQL 库存并更新为 COMPENSATING。未建单的预占释放不返还 MySQL 库存。所有 Redis 回补都由对账任务处理。

## 对账和 Redis 凭证

- `seckill:reservation:{voucherId}`：Hash，userId → orderId，用于资格与预占归属。
- `seckill:pending`：ZSet，member 为 voucherId:userId:orderId。它是业务对账索引，不是 RocketMQ Pending List。
- `seckill:released:{orderId}`：Redis 回补凭证，值为完整 member，不设置 TTL。需覆盖所有可能的重试、消息重放和恢复窗口，不能随意删除。
- `seckill:pending:retries`：旧版遗留键；新流程不写入，仅在清理旧 pending 时删除对应字段。

每轮先检查超时 pending：订单已提交时清除该索引；未发现订单时保留预占，等待 RocketMQ 消费或死信登记。**超时本身不是消费失败的证明**，不按超时直接返库。随后按处理状态分别扫描 DEAD_LETTER 和 COMPENSATING：前者在事务中重新仲裁并进入 COMPENSATING，后者通过 Lua 校验归属、回补库存并写入按订单唯一的凭证，再把处理状态更新为 COMPENSATED。

库存和凭证由一次 MSET 写入；如果后续资格或索引清理出错，再次执行看到凭证只补做清理，不会再次加库存。预占缺失或归属不符且没有凭证时无法判断是否回补过，保留 COMPENSATING 并记录错误，不盲目加库存。Redis 脚本无跨 MySQL 的事务，也不提供运行时异常回滚；凭证与库存应一并持久化并纳入备份和恢复。

任务分批处理，状态扫描游标保存在 `job:seckill-reconcile:cursor`，任务锁保护游标；数据库条件更新和 Redis 凭证负责业务幂等。失败记录保留，下一轮继续，错误写入应用与 XXL 任务日志。当前不接入独立通知渠道。

## 配置

```yaml
hmdp:
  seckill:
    reconcile:
      enabled: true
      interval: 1m
      pending-timeout: 5m
      batch-size: 100
```

`interval` 只用于本地调度；XXL 周期在 Admin 配置。订单创建重试次数由 `hmdp.rocketmq.max-reconsume-times` 控制。死信消费者使用独立组读取 `%DLQ%hmdp-voucher-order-consumer`，首次从仍保留的最早消息读取；登记失败会返回消费失败，不提前确认。

如果事务半消息最终未提交、消息超过 Broker 保留期、死信登记链路不可用或消息体无法解析，订单可能只有 Redis pending、没有死信记录。任务会持续保留并记录日志，**不会自动补发或释放**；需要监控积压并核查。库存汇总比对尚未实现。

## 数据库升级

新库使用 `db/hmdp.sql` 或 `db/sharding-schema.sql`。旧库先停止写入、消费者和调度，备份后执行 `db/voucher-order-process-migrate.sql`；同实例双库分片使用 `db/voucher-order-process-sharding-migrate.sql`。单库脚本也会补建并回填既有订单路由表，需确认路由不一致数量为 0。先核对迁移的记录和状态，再手动执行脚本末尾注释的删索引、删 `redis_stock_restored` 语句。MySQL DDL 会隐式提交，不应视为单个原子事务。

旧版 Redis 已返库、但旧数据库标记仍为 0 的记录没有新凭证，会保留 COMPENSATING，需人工核对。不能将这类记录直接重新加库存。部署前统一切换所有实例，并保留处理终态直至所有旧消息和人工重放窗口结束。

分片模式下处理表按 order_id 路由，建单与关单涉及多个分片时依赖现有 XA 配置；按状态扫描会跨分片合并。当前 Redis 预占脚本使用全局 pending，适用于项目现有单节点 Redis，不能未经同槽改造直接迁往 Redis Cluster。

## 验证

单元测试覆盖死信登记、状态仲裁、对账和关单。显式开启 `HMDP_SECKILL_RESERVATION_IT=true` 可在专用 Redis 16389 验证实际 Lua；`HMDP_SHARDING_IT=true` 在专用 MySQL 13316 验证分片与 XA；`HMDP_ORDER_PROCESS_MYSQL_IT=true` 验证单库及分片迁移；`HMDP_ROCKETMQ_DLQ_IT=true` 在专用 RocketMQ 19876/20911 验证死信自动订阅；`HMDP_ORDER_RECOVERY_E2E=true` 联合三个隔离实例验证死信登记到对账回补的完整流程。启用前必须确保测试端口指向隔离实例。
