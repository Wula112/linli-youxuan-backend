# 邻里优选——本地生活服务平台

基于 Spring Boot、MyBatis-Plus、MySQL 和 Redis 的本地生活服务平台后端，提供用户登录、商铺查询、探店笔记、关注、优惠券和秒杀订单等功能。

## 项目结构

项目包含四个 Maven 模块，整体作为一个 Spring Boot 应用部署：

| 模块 | 职责 |
| --- | --- |
| `hmdp-common` | 共享 DTO、常量、用户上下文与通用工具 |
| `hmdp-framework` | 缓存、Redis 锁、限流、ID 生成与 XXL-JOB 基础配置 |
| `hmdp-sharding` | ShardingSphere 数据源与 XA 接入 |
| `hmdp-core-service` | 接口、业务服务、数据访问、定时任务与消息队列 |

详细目录和模块说明见[项目结构说明](docs/project-structure.md)。

## 开发环境

- Java 21、Maven
- Spring Boot 4.1.1、MyBatis-Plus 3.5.17、Redisson 4.7.0
- MySQL、Redis、RocketMQ 5.x

## 本地运行

1. **初始化数据库**

   执行 `hmdp-core-service/src/main/resources/db/sharding-schema.sql`，创建 `hmdp_0` 和 `hmdp_1`。应用默认启用 `sharding` profile，使用两库两表与 XA。

   已有单库数据可执行 `sharding-migrate-from-hmdp.sql` 迁移。示例数据和迁移步骤见[分库分表说明](docs/database-sharding.md)。

2. **准备应用配置**

   ```powershell
   Copy-Item hmdp-core-service/src/main/resources/application.example.yaml hmdp-core-service/src/main/resources/application.yaml
   $env:DB_PASSWORD = '你的本地数据库密码'
   ```

   按实际环境修改数据库、Redis 和 RocketMQ 配置。

3. **启动依赖服务**

    - Redis：`127.0.0.1:6379`
    - RocketMQ NameServer：`127.0.0.1:9876`

   使用图片上传功能前，还需在 `SystemConstants.java` 中配置 `IMAGE_UPLOAD_DIR`，并准备静态文件服务。

4. **构建并启动**

   ```shell
   mvn -DskipTests package
   java -jar hmdp-core-service/target/hm-dianping-0.0.1-SNAPSHOT.jar
   ```

   默认服务端口为 `8081`。

## 商铺缓存与布隆过滤器

商铺详情按“缓存 → 布隆过滤器 → 数据库”的顺序查询。过滤器判断商铺不存在时直接返回；数据库未查到商铺时，缓存空值 2 分钟。

应用启动时预热布隆过滤器。新增商铺时同步写入过滤器，新增或更新成功后删除缓存，并延迟 1 秒再次删除。过滤器默认容量为 100 万，目标误判率为 1%。

直接向数据库导入商铺后，需重启应用重新预热。调整过滤器容量或误判率时，需清除原过滤器及相关标记后重启。

## 分类与优惠券多级缓存

商铺分类和优惠券列表默认按“Caffeine → Redis → MySQL”的顺序查询，未命中时逐级回源并回填。

| 数据 | 本地缓存 | Redis 缓存 |
| --- | --- | --- |
| 商铺分类列表 | 写入 5 秒后可刷新，10 分钟后过期 | 默认 30 分钟过期 |
| 商铺优惠券列表 | 最多缓存 1000 个商铺；写入 5 秒后可刷新，10 分钟后过期 | 默认 10 分钟过期 |

空列表也会写入缓存，Redis 中的空列表默认 2 分钟过期。缓存更新通过 RocketMQ 广播通知各实例清理本地缓存。

### 查询模式

两类列表均可配置为以下模式：

| mode | 查询路径 |
| --- | --- |
| `mysql` | 直接查询 MySQL |
| `redis` | Redis → MySQL |
| `caffeine` | Caffeine → Redis → MySQL |

默认配置：

```yaml
hmdp:
  cache:
    shop-type:
      mode: caffeine
      maximum-size: 1
      refresh-after-write: 5s
      expire-after-write: 10m
      redis-ttl: 30m
    voucher-list:
      mode: caffeine
      maximum-size: 1000
      refresh-after-write: 5s
      expire-after-write: 10m
      redis-ttl: 10m
```

也可通过启动参数临时切换，例如：

```shell
java -jar hmdp-core-service/target/hm-dianping-0.0.1-SNAPSHOT.jar --hmdp.cache.shop-type.mode=redis --hmdp.cache.voucher-list.mode=redis
```

分类列表接口为 `GET /shop-type/list`，优惠券列表接口为 `GET /voucher/list/{shopId}`。设置 `hmdp.cache.mq.enabled: false` 可关闭缓存失效广播。详见[缓存一致性说明](docs/cache-consistency.md)。

## RocketMQ 秒杀异步下单

秒杀请求通过 RocketMQ 事务消息处理。Redis Lua 完成一人一单校验和库存预扣，校验通过后提交消息；消费者在同一数据库事务中扣减库存并创建订单。

接口返回的订单号表示请求已受理。消费失败会自动重试，重试耗尽后进入死信主题；重复消息不会重复扣减库存。

已有数据库需按升级说明配置订单唯一索引。唯一约束仅作用于未关闭订单，订单关闭后可再次抢购。详见[秒杀异步下单说明](docs/voucher-order-mq.md)。

## 秒杀订单对账

系统通过 Redis 记录库存预占和待处理订单。用户查询或对账发现订单未创建时，会直接尝试建单。

对账任务默认核查 2 分钟前的预占，并返还超时关单的库存。消费重试耗尽时触发熔断和 XXL-JOB 失败告警，已受理订单继续处理。

配置项为 `hmdp.seckill.reconcile`。详见[订单对账说明](docs/seckill-order-reconcile.md)。

## 订单支付与自动关单

订单创建后，用户可在 15 分钟内模拟支付。超时订单由 RocketMQ 定时消息和数据库扫描任务关闭。关单后返还库存并释放购买资格，历史订单保留。

已有库需迁移订单处理记录并核对旧返库标记，再删除订单表的旧返库字段。配置与测试方法见[支付说明](docs/voucher-order-payment.md)和[自动关单说明](docs/voucher-order-timeout.md)。

## XXL-JOB 调度与缓存预热

项目使用 XXL-JOB 3.4.2，默认采用本地调度。通过 `--spring.profiles.active=sharding,xxl` 启用 XXL-JOB 后，由 Admin 统一调度缓存预热、订单对账和关单补偿任务。

缓存预热默认处理正在进行或未来 10 分钟内开始的秒杀活动，每轮最多预热 100 家商铺。需单独准备 Admin、通信令牌和任务配置，Admin 重试次数设为 `0`。

配置示例和验证方法见[XXL-JOB 接入说明](docs/xxl-job.md)。

## 秒杀请求限流

秒杀接口使用 Redis 滑动窗口限流，默认所有实例和优惠券共用每秒 100 次的额度。可通过 `hmdp.seckill.rate-limit.window` 和 `limit` 调整，也支持按用户或 IP 限流。

配置与测试见[限流说明](docs/seckill-rate-limit.md)。

## 测试

```shell
mvn test
```

测试会连接 MySQL、Redis，并写入缓存或商铺 GEO 数据，请使用专用测试环境。

无需外部服务的单元测试可单独运行：

```shell
mvn "-Dtest=*Test" test
```
