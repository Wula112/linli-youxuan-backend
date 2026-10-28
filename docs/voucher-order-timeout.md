# 延迟自动关单

默认支付窗口15分钟，使用固定消息TTL队列与DLX触发关单，MySQL条件更新决定是否允许关闭，数据库扫描兜底。未支付订单关闭后返还库存，允许原用户再次抢购同一张券，历史订单保留。

## 消息与事务

```text
订单事务首次建单成功并提交
    → OrderTimeoutPublisher（Confirm + Returns + 有限重试）
    → order.delay.exchange → order.delay.queue（固定消息TTL，默认15分钟，无消费者）
    → 过期后转发至 order.close.exchange → order.close.queue
    → OrderTimeoutConsumer → closeExpiredOrder
    → 正常返回AUTO ACK；异常按原容器配置重试，耗尽转 order.close.dlq
```

消息只保存订单ID。提交回调内发布失败只记录日志，不能撤销已提交的订单。MQ通知比实际expire_time晚到或者提前到达时，都以数据库截止时间为准；提前通知不会关单，之后由数据库扫描处理。

关单方法显式划分提交阶段，避免在MySQL提交前释放Redis资格：

1. 在数据库事务中执行 `status=1 AND expire_time<=NOW(3)` 条件更新为4，并设置 `redis_stock_restored=0`。
2. 仅本次状态更新成功时执行MySQL库存+1，与关单一同提交；库存更新失败则整个事务回滚。
3. 数据库提交完成后，复用 `seckill-rollback.lua` 验证预占Hash归属，原子执行Redis库存+1、删除该订单用户资格、清理旧pending和补发计数。
4. Redis处理成功后，单独提交 `redis_stock_restored=1`。若进程在提交、Lua执行或标记写入之间退出，扫描任务会继续补偿。

支付的条件更新只允许 `status=1 AND expire_time>NOW(3)` 变为2。支付与关闭在MySQL同一行上竞争，已支付订单不会被关闭或返库。关单不依赖额外分布式锁。

重复关单不会再次返还MySQL库存。Lua只在预占Hash仍匹配原orderId时返还Redis库存；重复调用或旧订单的迟到补偿不会修改新预占。Lua已经执行、但响应或数据库标记丢失时，再次调用返回0也可以完成补偿标记。

## 再次抢购与唯一约束

原 `(user_id,voucher_id)` 唯一索引会阻止已关闭订单的用户再次建单，现改为：

```sql
active_order = CASE WHEN status = 4 THEN NULL ELSE 1 END
UNIQUE (user_id, voucher_id, active_order)
```

active_order是数据库生成列，不由应用赋值。MySQL唯一索引允许多个NULL，因此关闭历史可以保留多条；同一用户、同一张券最多有一笔未关闭订单。建单前的数量校验也排除状态4。

再次抢购需等Redis返库并释放资格完成。此时原已关闭订单记录仍存在，同订单号重复消息不会重新创建订单。

## 定时兜底

`job/OrderTimeoutJob` 分别扫描到期未支付订单和已关闭但Redis返库未完成的订单，复用同一关单方法。默认本地模式使用独立单线程 `order-timeout-` 调度器；XXL 模式停用本地入口，由 `orderTimeoutCompensate` Handler 触发。业务幂等继续依靠数据库条件更新和Lua归属校验。

每种扫描每次最多10批，按订单ID翻页；两个游标保存在 Redis 的 `job:order-timeout:cursor` Hash，共用任务锁保护，因此切换执行器后可以继续扫描。到尾后重置游标，重新检查此前失败的记录。单条异常会记录日志并继续处理后面的订单，同时将本轮向 XXL-JOB 报为失败。

单轮默认30秒软预算，两类扫描各分配至多一半时间，避免未支付订单积压挤占返库补偿；到时停止处理新记录，保存游标供后续轮次继续。`timeout-scan-interval` 只影响本地模式，XXL 周期在 Admin 配置，详情见 [XXL-JOB 接入说明](xxl-job.md)。

关闭扫描只会停用兜底任务，MQ正常触发仍然工作。数据库扫描不使用Redis预占pending，未建单补发与已建单超时关闭是两个流程。

## 配置与升级

```yaml
hmdp:
  order:
    pay-timeout: 15m
    timeout-scan-enabled: true
    timeout-scan-interval: 1m
    timeout-scan-batch-size: 100
```

发送确认、Returns和发布重试复用 `hmdp.mq.voucher-order`；消费确认、有限重试复用现有RabbitMQ默认容器配置。

已有数据库在暂停写入后，先确保原唯一索引及 `voucher-order-payment.sql` 已应用，再执行一次 `hmdp-core-service/src/main/resources/db/voucher-order-timeout.sql`。不要重新导入含DROP TABLE的hmdp.sql。新建数据库的hmdp.sql已经包含生成列、新唯一索引、返库标记及扫描索引。

升级脚本将历史状态4订单的返库标记设为完成，避免在缺少历史返库凭据时自动重复加库存；这些历史库存和旧Redis资格需单独核查。本次新关闭订单则显式标记为未完成，纳入自动补偿。

队列TTL从消息入队起算，现存队列的声明参数不能通过简单修改配置直接变更。调整pay-timeout时，需要安排已有延迟队列的参数迁移，避免RabbitMQ的PRECONDITION_FAILED；有积压时不能直接删除队列。数据库expire_time和扫描始终是最终判断依据。

保留Redis库存与预占数据的持久化保障；这里的幂等依赖预占归属，不能随意删除Hash后再凭总库存猜测返还。MySQL、Redis和RabbitMQ仍不构成单个原子事务，扫描用于恢复跨存储的未完成步骤。

## 验证

单元测试覆盖提交后发布、回滚不发送、Confirm NACK/Return/超时重试、关单与返库提交顺序、失败恢复以及扫描游标。

`VoucherOrderFlowIT` 使用专用MySQL3308、Redis6388和RabbitMQ5678，显式设置 `HMDP_VOUCHER_ORDER_IT=true` 后运行。新增真实TTL/DLX自动关闭、支付后通知不关单、漏消息扫描兜底、Redis故障补偿、返库标记失败后重新抢购、并发关闭、失败重试进死信和支付/关闭竞争场景。测试使用随机数据库，延迟队列使用短TTL，结束后清理测试拓扑。
