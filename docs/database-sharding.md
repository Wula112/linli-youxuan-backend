# 两库两表与 XA 实现

更新：2026-10-27。本文描述当前代码，取代之前学习方案中的实施建议。

## 实现范围

采用 ShardingSphere-JDBC 5.5.3 + Atomikos XA，继续使用 Spring Boot 4.1.1 / MyBatis-Plus 3.5.17。没有引入 Outbox、对账表、补偿表、事件表或新的消息流程。已有 Redis、RabbitMQ、支付、自动关单和定时任务继续沿用。

本次实现已讨论的店铺与交易分片；用户、笔记、关注等现有功能明确保留为单表，避免同时扩展其他业务域。

| 逻辑表 | 选库 | 选表 |
| --- | --- | --- |
| tb_shop | id % 2 | (id / 2 向下取整) % 2 |
| tb_voucher | id % 2 | (id / 2 向下取整) % 2 |
| tb_seckill_voucher | voucher_id % 2 | (voucher_id / 2 向下取整) % 2 |
| tb_shop_voucher_index | shop_id % 2 | (shop_id / 2 向下取整) % 2 |
| tb_voucher_order | user_id % 2 | voucher_id % 2 |
| tb_voucher_order_router | order_id % 2 | (order_id / 2 向下取整) % 2 |

每个逻辑表分成 `ds_0`、`ds_1` 中的 `_0`、`_1`，共 4 张物理表。规则保存在 `hmdp-core-service/src/main/resources/sharding.yaml`。

`tb_shop_type` 固定在 `ds_0`，没有广播表。`tb_user`、`tb_user_info`、`tb_blog`、`tb_blog_comments`、`tb_follow`、`tb_sign` 同样固定在 `ds_0`。两个库放在同一 MySQL 实例可完成本地学习；也可通过两个 JDBC URL 指向独立实例。

## 查询与写入

- 店铺详情按 ID 精确路由。附近店铺保持 Redis GEO → 店铺 ID → 批量回表，并在应用层恢复 GEO 顺序。分类与名称查询暂时保留有界跨分片分页，增加稳定的 ID 排序，没有额外引入搜索服务或分类索引。
- 券与库存按相同券 ID 路由，显式配置绑定关系。店铺券列表先按 `shop_id` 查询 `tb_shop_voucher_index`，再用 `id IN (...)` 关联读取券与库存。空索引直接返回空列表，不执行空 IN SQL。状态直接读取券主表，索引只保存 `(shop_id, voucher_id)`，不复制状态与库存。
- 发布、修改归属、删除券时同步维护上述关联，沿用现有事务和缓存失效。当前店铺券接口没有分页参数，因此保留返回该店铺全部上架券的行为。一次批量回表仍可能涉及多个甚至全部分片，但每个分片读取的是已知券 ID。
- 创建订单时同步写 `tb_voucher_order_router(order_id,user_id,voucher_id)`。按订单号查询、支付、关单先查目录，再在订单 SQL 中携带两个分片键。携带三个 ID 的现有对账消息直接使用完整条件查询。
- 自动关单的数据库扫描仍有批次与游标限制，允许跨分片查询，由中间件合并 `ORDER BY id LIMIT ...`。订单详情与更新不使用全路由。不要新增绕过目录的 `selectById(orderId)` / 只按订单 ID 的更新。
- 订单保留最新代码中的生成列 `active_order` 和唯一键 `(user_id,voucher_id,active_order)`；关闭订单后仍允许再次购买。不会退回“所有历史订单永久限制一人一单”的旧约束。

## XA 边界

`sharding.yaml` 的 `transaction.defaultType=XA`、`providerType=Atomikos` 负责管理物理数据库分支。Spring 的 `@Transactional` / `TransactionTemplate` 控制逻辑连接，ShardingSphere 将连接事务交给 XA 实现。

- 下单：扣数据库库存、保存订单、保存订单目录共同提交/回滚。
- 关单：条件关闭订单、返还数据库库存共同提交/回滚；沿用现有提交后 Redis 返库与标记流程。
- 保存券：券主表与店铺关联索引共同提交/回滚；秒杀发布还包括同路由库存表。

XA 没有把 Redis 或普通 RabbitMQ 发送纳入数据库事务，继续保留现有实现的边界。此次不追加其他可靠事件机制。

Atomikos 的恢复日志保存在本地目录，默认 `./logs/xa/hmdp-0`。这是事务恢复所需的文件，不是新增业务表。不要在重启时清理尚有未决事务的日志；同一实例重启保持 XA 节点名与日志目录不变，不同运行实例使用不同节点名和目录。

XA 适配层使用 `javax.transaction` API，因此显式保留 `javax.transaction-api:1.3`，不使用 Spring 的 Jakarta JTA 管理器桥接，也不降级 Spring Boot。

## 雪花 ID

统一使用 MyBatis-Plus 的 `DefaultIdentifierGenerator`，不再通过 Redis 日计数生成新订单号。店铺、券、用户、笔记、评论、关注的主键使用 `ASSIGN_ID`；订单入口通过同一个 `IdentifierGenerator` 生成订单号。用户详情的 `user_id` 和库存的 `voucher_id` 是关联键，沿用已有 ID，不再独立自增。分类字典继续保留固定/自增分类编号。

`ID_WORKER_ID` 与 `ID_DATACENTER_ID` 均在 0..31 之间；每个同时运行的实例必须使用不同的编号组合。默认单实例使用 `(0,0)`。无需新增 ID 服务、Redis 分配器或协调中心。

HTTP JSON 将 Long 序列化为字符串，避免雪花 ID 在 JavaScript 中丢失精度。Java 实体与数据库仍使用 Long / BIGINT。已有 ID 原样迁移，分片规则按实际数值计算；简单取模不承诺雪花 ID 在各种生成速率下严格均匀。

## 初始化与迁移

应用默认使用 `sharding` profile，`application.yaml` 继续提供 Redis、MQ 及本机凭据。首次启动前准备分片结构：

```sql
SOURCE hmdp-core-service/src/main/resources/db/sharding-schema.sql;
```

这个脚本只创建 `hmdp_0`、`hmdp_1` 和物理表，不会自动删除或改写旧 `hmdp`。请使用空的目标库；之前粘贴的参考脚本与当前实体/DDL 不一致，不要混用。脚本没有自动执行的 Spring 初始化配置。

若要保留当前单库数据：停止应用写入，确认旧 `hmdp` 已具备当前支付、超时关单字段，然后在同一 MySQL 实例执行：

```sql
SOURCE hmdp-core-service/src/main/resources/db/sharding-migrate-from-hmdp.sql;
```

迁移保留旧主键，按规则复制店铺、券、库存、订单，补建店铺券索引与订单目录，并把其余单表复制到 `hmdp_0`。目标应为空；脚本不会忽略主键冲突，也不会覆盖已有数据。中途出错请在该连接回滚，不要继续提交。此脚本不用于跨实例在线迁移。

新项目需要示例数据时，可以先在专用 `hmdp` 导入原始 `db/hmdp.sql`，再执行上述复制脚本。没有示例数据时可直接在空分片中通过现有接口创建数据。

连接默认指向本机 MySQL 3306；两个节点的用户/密码默认继承 `spring.datasource.username/password`，可单独覆盖：

```powershell
$env:DB_0_URL = 'jdbc:mysql://127.0.0.1:3306/hmdp_0?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai'
$env:DB_1_URL = 'jdbc:mysql://127.0.0.1:3306/hmdp_1?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai'
$env:DB_PASSWORD = '本机密码' # 也可使用 DB_0_PASSWORD / DB_1_PASSWORD 分别配置
$env:ID_WORKER_ID = '0'
$env:ID_DATACENTER_ID = '0'
$env:XA_NODE_ID = 'hmdp-0'
$env:XA_LOG_DIRECTORY = './logs/xa/hmdp-0'
$env:SHARDING_SQL_SHOW = 'true' # 学习时查看逻辑 SQL 和实际 SQL
mvn spring-boot:run
```

## 验证

无需外部服务的单元测试：`mvn "-Dtest=*Test" test`。

`ShardingXaIT` 只连接专用本机 MySQL 13316，初始化测试实例的 `hmdp_0/hmdp_1`，不读取业务连接配置。它会清空这些测试分片，请勿把该端口指向业务数据库。Redis 与 MQ 使用测试替身。

```powershell
$env:HMDP_SHARDING_IT = 'true'
mvn '-Dtest=ShardingXaIT' test
```

集成测试覆盖四个物理位置、单表分类、跨分片券/库存关联、店铺券索引、雪花 ID、订单目录与支付、跨库异常回滚、关单返库及跨分片扫描合并。生产环境断电、网络分区后的 XA 恢复仍需要在实际部署拓扑验证。
