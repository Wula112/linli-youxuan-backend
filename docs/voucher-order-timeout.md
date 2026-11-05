# 超时自动关单

默认支付窗口为 15 分钟。RocketMQ 定时消息负责及时触发，XXL-JOB 的 `orderTimeoutCompensate` 扫描到期未支付订单兜底；本地调度模式使用对应本地定时任务。所有入口调用相同的关单方法。

关单先锁定 `tb_voucher_order_process`，并在同一数据库事务中：

1. 条件更新订单：仅 `status=1 AND expire_time<=NOW(3)` 能变为已关闭。
2. 仅首次成功关单时返还 MySQL 库存。
3. 将处理状态从 CREATED 更新为 COMPENSATING。

Redis 不在上述数据库事务中更新。对账任务 `seckillOrderReconcile` 扫描 COMPENSATING，执行 Redis 幂等回补并在确认成功后改为 COMPENSATED；失败保留原状态，后续轮次继续。订单表不再使用 `redis_stock_restored` 字段。

支付只允许未支付且未到期的订单更新；支付和关单在订单行条件更新上竞争。重复关单不会多次返还 MySQL 库存；Redis 按订单释放凭证防止重复返库，并校验资格归属，旧订单的迟到补偿不能删除新订单资格。

已关闭订单仍保留历史，`active_order = CASE WHEN status = 4 THEN NULL ELSE 1 END` 和 `(user_id,voucher_id,active_order)` 唯一索引允许关闭后再次抢购。需等 Redis 资格释放后才能再次通过预扣校验。

## 调度与升级

`hmdp.order.timeout-scan-enabled` 控制本地扫描兜底，`timeout-scan-interval` 默认 1 分钟、`timeout-scan-batch-size` 默认 100。XXL 模式下在 Admin 配置 `orderTimeoutCompensate` 与 `seckillOrderReconcile`，两者都要启用。扫描游标保存在 Redis，业务状态以数据库为准。

已有库必须先迁移订单处理状态，再删除旧字段；单库和分片脚本及历史返库不确定的处理见[订单对账说明](seckill-order-reconcile.md)。`voucher-order-timeout.sql` 是更早版本的历史升级脚本；从该版本升级后仍需运行新的处理记录迁移。

定时消息可能提前、延后或发送失败，数据库截止时间和扫描始终作为关单条件；关闭后 Redis 返库可能延迟到下一轮对账。正常订单状态的唯一约束和分片路由规则保持不变。
