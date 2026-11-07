# 秒杀订单处理与对账

预占成功并返回订单号后，系统持续尝试创建订单。用户查询发现订单未落库时会直接尝试建单；XXL-JOB 的 `seckillOrderReconcile` 也会恢复超时预占和死信订单，不再补发消息或因死信返库。只有订单超时关闭后才返还 Redis 库存。

## 处理记录和状态

`tb_voucher_order_process` 每笔订单一行，字段为 `order_id`、`user_id`、`voucher_id`、`status`，不记录失败原因或重试次数。

```text
PENDING ──────────────建单成功──────────────> CREATED
   │                                             │
   └──死信登记──> DEAD_LETTER ──恢复建单─────────┘
                                                 │
                           订单超时关单、返还 MySQL 库存
                                                 ▼
                                     COMPENSATING ──Redis 返库──> COMPENSATED
```

MQ 消费、用户查询和对账恢复共用建单事务：先插入或锁定处理记录，再扣减 MySQL 库存、写入订单与路由、更新 CREATED。重复执行不会重复建单；数据库异常或库存数据冲突时保留请求，等待下轮重试和问题修复。支付时间从成功建单时开始计算。

超时关单在同一数据库事务中关闭未支付订单、返还 MySQL 库存并更新 COMPENSATING。对账核实订单确为已关闭后才允许 Redis 返库；Lua 以订单唯一凭证保证重复执行不会多次返库。

## Redis 索引与对账

- `seckill:reservation:{voucherId}`：Hash，userId → orderId，校验预占归属。
- `seckill:pending`：ZSet，member 为 voucherId:userId:orderId，用于发现尚未确认建单的请求。
- `seckill:requests`：Hash，orderId → 完整 member，供订单查询定位用户和优惠券；成功建单后与 pending 一起清理。
- `seckill:released:{orderId}`：Redis 返库凭证，不设置 TTL；必须覆盖迟到消息和重试窗口。
- `seckill:order:circuit`：秒杀熔断标记，无自动过期；值为首个死信消息 ID。

对账默认核查 2 分钟前的 pending，查到已提交订单则清理索引，未查到则直接尝试建单；之后扫描 DEAD_LETTER 和 COMPENSATING。领取会刷新 pending 的分数，失败后到下一个等待窗口再试。任务锁保护游标，处理记录和 Redis 凭证负责业务幂等。

库存和返库凭证由一次 MSET 写入。后续清理失败时，重试看到凭证只做清理，不再加库存；预占归属不符且没有凭证时保留状态并报错，不盲目返库。当前不做按券库存总量汇总比对。

## 熔断与告警

下单消息重试耗尽进入死信后，消费者先持久化熔断标记，再登记死信。熔断打开后，秒杀入口拒绝新请求；Redis 预占脚本也在扣库存前检查熔断，封住入口检查之后的并发窗口。已经预占的请求仍可通过 MQ、查询和对账建单。

对账任务发现熔断时继续处理已受理请求，同时向 XXL-JOB 上报失败。需在 Admin 的 `seckillOrderReconcile` 任务中配置告警收件人，并在 Admin 配置可用的 SMTP；未配置时只会有失败状态和日志，不会发送邮件。熔断保持期间每轮都会上报失败，可能重复告警；本地调度没有 XXL 邮件告警。配置项见 [XXL-JOB 3.4.2 Admin 示例](https://github.com/xuxueli/xxl-job/blob/3.4.2/xxl-job-admin/src/main/resources/application.properties)。

排查消费失败、确认依赖恢复并检查 pending/死信积压后，由运维手动删除 `seckill:order:circuit` 才恢复受理；没有自动半开或自动恢复。重新出现死信会再次熔断。

## 配置与升级

`hmdp.seckill.reconcile.pending-timeout` 默认为 `2m`，每批默认 100 条；本地 `interval` 默认为 1 分钟，XXL 周期由 Admin 配置。消费重投次数仍由 `hmdp.rocketmq.max-reconsume-times` 控制。

从上一版升级时先停止所有秒杀入口、消费者和对账任务，处理或保留历史数据，并为已有 pending 回填订单反向索引。以 PowerShell 为例，将连接参数替换为目标 Redis 后循环运行，直到游标为 `0`：

```powershell
$cursor = '0'
do {
    $cursor = (& redis-cli --raw --eval 'hmdp-core-service/src/main/resources/seckill-request-index-backfill.lua' 'seckill:pending' 'seckill:requests' ',' $cursor 500).Trim()
    if ($LASTEXITCODE -ne 0 -or $cursor -notmatch '^\d+$') { throw '回填失败，请检查 Redis 返回结果' }
} while ($cursor -ne '0')
```

停止写入期间完成回填后，再统一启动新版本。旧版 COMPENSATING 但没有订单的记录，仅在预占仍属于该订单且没有返库凭证时自动恢复建单；已返库或归属不明的历史记录不能安全地自动重建，需人工核查库存。更早版本的数据库迁移仍见 `db/voucher-order-process-migrate.sql` 和 `db/voucher-order-process-sharding-migrate.sql`。

订单最终创建依赖 Redis 中的受理记录仍在、数据库及库存数据能够恢复正常；持续故障期间查询返回“订单正在处理中”。当前预占脚本使用全局 pending，适用于单节点 Redis，不能未经同槽改造直接迁往 Redis Cluster。

## 验证

单元测试覆盖查询建单、熔断、状态仲裁和关单。`HMDP_SECKILL_RESERVATION_IT=true` 验证 Redis Lua 和旧索引回填；`HMDP_SHARDING_IT=true` 验证分片 XA 和并发建单；`HMDP_ORDER_RECOVERY_E2E=true` 联合专用 MySQL、Redis、RocketMQ 验证死信熔断、继续建单和关单返库。启用前确认测试端口指向隔离实例。
