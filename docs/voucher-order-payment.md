# 订单状态与模拟支付

本阶段实现首次建单状态初始化、本人订单查询和模拟支付。沿用现有状态值，不引入状态机框架、支付流水表或真实支付渠道。

## 状态规则

| 状态值 | 枚举 | 含义 |
| --- | --- | --- |
| 1 | UNPAID | 未支付 |
| 2 | PAID | 已支付 |
| 3 | USED | 已核销（原定义保留） |
| 4 | CLOSED | 已取消/关闭（原定义保留） |
| 5 | REFUNDING | 退款中（原定义保留） |
| 6 | REFUNDED | 已退款（原定义保留） |

本次开放 `UNPAID → PAID`。首次建单显式保存 UNPAID、创建时间和 expire_time，默认支付窗口15分钟，从消费端建单时计算。订单重复投递不会覆盖已有状态，也不会延长支付截止时间。

支付使用一条 MySQL 条件 UPDATE，同时校验订单号、当前登录用户、UNPAID 和 `expire_time > NOW(3)`，并保存支付方式与支付时间。并发重复支付只有一次能改变状态，其他请求核对当前状态后幂等返回成功；已支付订单即使过了截止时间，重复支付仍返回成功，首次支付方式和时间不会改变。

支付截止时间以数据库时间判断，相等时已到期。已关闭及其他不支持支付的状态无法转为 PAID。到期后由延迟消息或数据库扫描条件更新为CLOSED，返还库存并释放本订单资格，详见[自动关单说明](voucher-order-timeout.md)。

## 配置与数据库升级

```yaml
hmdp:
  order:
    pay-timeout: 15m
```

已有数据库应在暂停订单写入后执行一次 `hmdp-core-service/src/main/resources/db/voucher-order-payment.sql`，再启动新版代码。脚本新增 expire_time，按历史 create_time + 15分钟补齐，不修改历史状态。历史未支付订单若已超时，将无法继续支付。新建数据库的 hmdp.sql 已包含该字段。

配置修改只影响之后创建的订单，不改变已有订单的截止时间。

## 接口

以下接口均需要已有登录Token，只允许访问当前用户的订单，用户ID从服务端登录上下文读取。

```http
GET /voucher-order/{orderId}
authorization: <登录Token>
```

返回现有 Result 包装的订单对象，包含 status、expireTime、payTime 等字段。异步订单尚未落库时返回“订单不存在”，调用方可稍后查询；此查询结果不表示秒杀预占已取消。

```http
POST /voucher-order/pay/mock/{orderId}?payType=2
authorization: <登录Token>
```

payType默认1，沿用原定义：1余额、2支付宝、3微信。这里只记录模拟选择，不实际扣款，也不调用外部支付平台。成功返回 `Result.ok(orderId)`，失败使用已有 Result.fail 格式。

## 验证

单元测试覆盖状态初始化、重复消息不延长支付时间、幂等支付、超时/关闭/其他用户订单拒绝，以及配置绑定。

`VoucherOrderPaymentIT` 显式设置 `HMDP_VOUCHER_PAYMENT_IT=true` 后运行，使用专用MySQL 3308并创建随机测试库。实际执行迁移脚本和Mapper SQL，覆盖并发支付只能更新一次、到期拒绝、关闭拒绝、用户归属和首次支付记录不被覆盖。

延迟关单、扫描兜底及关闭后的库存返还已接入，允许原用户再次抢购。核销和退款仍未开放。
