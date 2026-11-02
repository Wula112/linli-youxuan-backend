# 黑马点评后端

基于 Spring Boot、MyBatis-Plus、MySQL 和 Redis 的点评项目后端，提供用户登录、商铺查询、探店笔记、关注、优惠券和秒杀订单等功能。

## 项目结构

父工程包含四个 Maven 模块，整体作为一个 Spring Boot 应用部署：

| 模块 | 职责 |
| --- | --- |
| `hmdp-common` | 共享 DTO、常量、订单状态、用户上下文与通用工具 |
| `hmdp-framework` | 缓存工具、Redis 锁、限流、ID 生成与 XXL-JOB 基础配置 |
| `hmdp-sharding` | ShardingSphere 数据源与 XA 接入 |
| `hmdp-core-service` | 启动类、接口、业务服务、实体、Mapper、业务缓存、定时任务与业务 MQ |

依赖方向为 `core-service → framework → common`、`core-service → common` 和 `core-service → sharding`。

核心模块中，`controller/` 提供 HTTP 接口，`service/` 实现业务逻辑，`mapper/` 和 `repository/redis/` 分别访问数据库与 Redis；消息适配位于 `mq/`，缓存位于 `cache/`，预热、对账和关单任务位于 `job/`。

配置、Mapper XML、业务 Lua 和数据库脚本位于 `hmdp-core-service/src/main/resources`，通用限流和锁 Lua 位于 `hmdp-framework/src/main/resources`。回归测试集中在核心模块。

模块边界、目录和迁移说明见[项目结构说明](docs/project-structure.md)。

## 开发环境

- Java 21
- Spring Boot 4.1.1
- MyBatis-Plus 3.5.17
- Redisson 4.7.0
- Maven
- MySQL、Redis、RocketMQ 5.x（事务消息与定时消息）

## 本地运行

1. **初始化数据库**

   执行 `hmdp-core-service/src/main/resources/db/sharding-schema.sql`，创建 `hmdp_0` 和 `hmdp_1`。应用默认启用 `sharding` profile，使用两库两表与 XA。

   已有单库数据可在停写后执行 `sharding-migrate-from-hmdp.sql` 迁移并保留原 ID。需要示例数据时，先将原始 `hmdp.sql` 导入专用 `hmdp` 数据库，再复制到空分片。详见[分库分表说明](docs/database-sharding.md)。

2. **准备应用配置**

   复制配置示例：

   ```powershell
   Copy-Item hmdp-core-service/src/main/resources/application.example.yaml hmdp-core-service/src/main/resources/application.yaml
   ```

   按实际环境修改数据库地址、用户名和 Redis 配置。数据库密码可通过环境变量设置：

   ```powershell
   $env:DB_PASSWORD = '你的本地数据库密码'
   ```

   `application.yaml` 已被 Git 忽略。修改通用配置时，请同步更新不含凭据的 `application.example.yaml`。

3. **启动 Redis**

   `RedissonConfig.java` 默认使用 `redis://127.0.0.1:6379`。更换地址或启用认证时，需同步修改 Redisson 与 Spring Redis 配置。

4. **启动 RocketMQ 5.x**

   NameServer 默认地址为 `127.0.0.1:9876`。秒杀异步下单使用事务消息，缓存失效使用广播主题。

5. **配置图片上传目录**

   在 `hmdp-common/src/main/java/com/hmdp/common/constant/SystemConstants.java` 中修改 `IMAGE_UPLOAD_DIR`，并配置静态文件服务。

6. **构建并启动**

   ```shell
   mvn -DskipTests package
   java -jar hmdp-core-service/target/hm-dianping-0.0.1-SNAPSHOT.jar
   ```

   默认服务端口为 `8081`。

## 商铺缓存与布隆过滤器

商铺详情按“缓存 → 布隆过滤器 → 数据库”的顺序查询：

- 缓存命中：直接返回。
- 缓存未命中且过滤器判断不存在：返回“商铺不存在”。
- 过滤器判断可能存在：查询数据库；未查到则缓存空字符串 2 分钟。

主要实现如下：

- `ShopBloomFilter` 复用 Redisson。应用启动时按 ID 每批加载 1000 个商铺，全部加载后启用拦截；预热失败或过滤器不可用时，按原流程查询缓存和数据库。
- 新增商铺仍调用 `shopService.save(shop)`。数据库生成 ID 后、事务提交前同步写入过滤器，写入失败则回滚新增。
- 新增或更新事务提交后，立即删除对应缓存，并在 1 秒后再次删除，降低并发查询回写旧值或空值的风险。事务回滚不触发删除；延迟时间由 `RedisConstants.CACHE_SHOP_DELETE_DELAY` 配置，单位为毫秒。
- 保留逻辑过期查询。首次缓存未命中时回源并写入逻辑过期缓存；空值缓存和互斥锁查询也使用同一过滤器。
- Redis Key 包括 `bloom:shop`、Redisson 配置 Key 和预热标记 `bloom:shop:ready`，均不设置过期时间。预计容量为 100 万，目标误判率为 1%，参数统一定义在 `RedisConstants` 中。
- 布隆过滤器不支持删除单个 ID，已删除商铺通过空值缓存处理。直接向数据库导入商铺后，需重启应用重新预热。调整容量或误判率时，应停止应用，删除过滤器、配置和预热标记后再启动。

## 分类与优惠券多级缓存

`queryTypeList()` 和 `queryVoucherOfShop(shopId)` 默认按“Caffeine → Redis → MySQL”的顺序查询。本地缓存未命中时读取 Redis 并回填本地；两级均未命中时查询数据库，再依次回填。同一实例内，相同 Key 的并发加载由 Caffeine 合并。

| 数据 | 本地缓存 | Redis 缓存 |
| --- | --- | --- |
| 商铺分类有序列表 | 整个列表使用一个 Key；写入 5 秒后可刷新，10 分钟后过期 | `cache:shop-type`，默认 30 分钟过期 |
| 商铺下的优惠券列表 | 按商铺 ID 缓存，最多 1000 个商铺；写入 5 秒后可刷新，10 分钟后过期 | `cache:shop:voucher:{shopId}`，默认 10 分钟过期 |

两类列表共用 `LocalCacheClient<K, T>`，其中 `K` 为本地缓存 Key 类型，`T` 为列表元素类型。该工具统一处理缓存模式、Caffeine 容量与刷新、Redis JSON 读写、数据库回源和空列表缓存。空列表也会写入两级缓存，Redis 中的空列表默认 2 分钟过期。

`ShopTypeCacheService` 和 `VoucherListCacheService` 分别提供缓存配置、Redis Key、元素类型和数据库查询函数。分类按 `sort` 升序查询，优惠券按商铺 ID 查询。优惠券缓存初始化示例：

```java
voucherListCache = new LocalCacheClient<>(stringRedisTemplate, cacheProperties, Voucher.class,
        shopId -> CACHE_SHOP_VOUCHER_KEY + shopId, voucherMapper::queryVoucherOfShop);
```

工具内部使用 `LoadingCache`。`@PostConstruct initCache()` 只初始化缓存容器和加载规则，首次查询时才读取 Redis 或数据库。达到刷新时间后，下次访问会触发异步刷新并返回旧值；刷新仍先查询 Redis，未命中时才回源 MySQL。

### 查询模式与压测

`ShopTypeCacheProperties` 和 `VoucherCacheProperties` 继承 `LocalCacheProperties`，可分别配置两类数据的查询模式、容量和过期时间：

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

| mode | 查询路径 |
| --- | --- |
| `mysql` | 直接查询 MySQL，不读写对应业务的两级缓存 |
| `redis` | Redis → MySQL，未命中时写回 Redis |
| `caffeine` | Caffeine → Redis → MySQL，未命中时逐级加载并回填 |

模式名不区分大小写。模式拼写错误、容量或时间不是正数时，应用会在启动时报错。配置修改后需重启生效。

分类列表接口为 `GET /shop-type/list`，优惠券列表接口为 `GET /voucher/list/{shopId}`。可通过启动参数切换模式：

```shell
java -jar hmdp-core-service/target/hm-dianping-0.0.1-SNAPSHOT.jar --hmdp.cache.shop-type.mode=mysql --hmdp.cache.voucher-list.mode=mysql
java -jar hmdp-core-service/target/hm-dianping-0.0.1-SNAPSHOT.jar --hmdp.cache.shop-type.mode=redis --hmdp.cache.voucher-list.mode=redis
java -jar hmdp-core-service/target/hm-dianping-0.0.1-SNAPSHOT.jar --hmdp.cache.shop-type.mode=caffeine --hmdp.cache.voucher-list.mode=caffeine
```

以上命令分别用于不同次启动，也可只覆盖其中一个模式。对比热缓存吞吐量前，先通过分类接口和相同商铺 ID 预热；冷缓存回源单独测量。`mysql` 模式只切换对应列表的查询路径，其他模块仍可能访问 Redis。

分类和优惠券列表通过 RocketMQ 广播失效消息：事务提交后先删除 Redis，再向 `hmdp-cache-invalidation` 主题发布消息。每个实例使用独立消费者组，收到消息后删除本实例的 Caffeine；消费成功返回 `CONSUME_SUCCESS`，失败则重投，重试耗尽后进入死信主题。

业务 MQ 代码位于 `com.hmdp.mq`，包括主题常量、客户端装配、失效消息、事务提交后的发布者和消费者。发布端使用 `tools.jackson` 序列化消息，通过 `TransactionMQProducer.send()` 发送并检查 `SendStatus`，重试次数由 `retry-times-when-send-failed` 控制。设置 `hmdp.cache.mq.enabled: false` 可关闭失效广播，用于基线压测。详见[缓存一致性说明](docs/cache-consistency.md)。

优惠券列表包含库存和活动时间，秒杀扣减仍使用 Redis Lua 流程；列表库存以 MySQL 已提交的数据为准。

## RocketMQ 秒杀异步下单

秒杀请求先发送事务半消息。Broker 落盘后，本地事务执行 Redis Lua，完成一人一单校验、库存预扣和事务标记写入：

- Lua 返回 `0`：提交消息。
- 库存不足或重复下单：回滚并丢弃消息。

消费者在同一数据库事务中扣减库存并创建订单，接口返回的订单号表示请求已受理。多个实例共享消费者组并竞争消费；成功后返回 `CONSUME_SUCCESS`，失败则重投并最终进入死信主题。本地事务结果未知时，Broker 通过事务标记回查决定半消息去留。

本地事务与回查位于 `mq/producer/`，预扣脚本和事务标记封装在 `repository/redis/SeckillReservation`。重复消息不会重复扣减库存；结果未知时的预扣回退与消费共用订单锁，并按订单号校验预占归属。旧 Redis Stream 实现以注释保留，当前默认使用 RocketMQ 事务消息。

已有数据库需按升级说明配置订单唯一索引。当前唯一约束仅作用于未关闭订单，订单关闭后可再次抢购。切换实现前，请先处理旧 Stream 的积压和 pending-list。详见[秒杀异步下单说明](docs/voucher-order-mq.md)。

## 秒杀订单对账

秒杀预占由库存 String、优惠券级 reservation Hash 和全局 pending ZSet 组成。Hash 用于一人一单校验及 `userId → orderId` 记录；pending 以完整的 `voucherId:userId:orderId` 定位请求，并在预扣时由 Lua 原子登记。订单提交成功后只删除 pending，保留购买资格。

`job/SeckillOrderReconcileJob` 默认每分钟核查 5 分钟前的记录，每批最多 100 条：

- 数据库已有订单：清理索引。
- 尚未建单且预占仍匹配：复用原消息进行有限补发。

领取脚本通过 ZSCORE 与 ZADD XX 原子延期，避免多实例重复领取或清理后重新加入。默认最多补发 5 轮；最后一轮结束后继续等待核查窗口，再获取订单锁并重新查库，确认未建单后原子释放预占。未获取到锁或依赖调用失败时，保留记录等待重试。补发上限设为 `0` 时只核查，不补发或自动释放。

配置项为 `hmdp.seckill.reconcile`。旧数据补齐、Redis 持久化和验证方法见[订单对账说明](docs/seckill-order-reconcile.md)。

## 订单支付与自动关单

订单创建后，用户可在 15 分钟内模拟支付。超时后，RocketMQ 定时消息触发关单，数据库扫描补充处理漏通知和未完成的 Redis 返库。关单后返还库存并释放本订单的购买资格，允许原用户再次抢购，历史订单保留。MQ 配置、消息、生产者和消费者统一位于 `mq/`。

升级前，按顺序执行支付截止时间和超时关单的增量 SQL。新唯一约束仅限制未关闭订单。配置、模拟支付接口和测试方法见[支付说明](docs/voucher-order-payment.md)与[自动关单说明](docs/voucher-order-timeout.md)。

## XXL-JOB 调度与缓存预热

执行器与业务应用一起部署，使用 XXL-JOB 3.4.2。默认使用本地调度；通过 `--spring.profiles.active=sharding,xxl` 启用 XXL-JOB 后，关闭原有两个本地定时入口，由 Admin 调度 Redis 预热、Caffeine 广播预热、pending 对账和关单补偿四个 Handler。

Redis 预热默认筛选正在进行或未来 10 分钟内开始的上架秒杀券，提取所属商铺并去重，每轮最多预热 100 家。整轮预热成功后，将名单写入 Redis，供各实例加载 Caffeine；没有活动时只预热分类，并发布空名单。

预热对临时故障最多进行两次指数退避重试；扫描任务通过持久化状态在后续周期恢复。Admin 重试次数应设为 `0`，避免多层重试叠加。订单事务、补发上限、延迟关单和返库规则保持不变。

需单独准备 Admin、通信令牌和任务配置。配置示例、路由、Cron、失败处理和验证方法见[XXL-JOB 接入说明](docs/xxl-job.md)。

## 秒杀请求限流

秒杀 Controller 入口使用自定义注解和 Redis 滑动窗口限流。默认所有实例、所有优惠券共用每秒 100 次的放行额度，可通过 `hmdp.seckill.rate-limit.window` 和 `limit` 调整。超限请求直接返回 `Result.fail`，不执行库存预占和消息发布。

限流模块同时支持 USER 和 IP 维度，配置与测试见[限流说明](docs/seckill-rate-limit.md)。

## 测试

运行全部测试：

```shell
mvn test
```

测试会连接 MySQL、Redis，并写入缓存或商铺 GEO 数据。请先完成环境配置，并使用专用测试数据。仅检查编译和打包时，可执行：

```shell
mvn -DskipTests package
```

以下单元测试无需启动外部服务，覆盖布隆过滤器、多级缓存、消息可靠性和延迟双删：

```shell
mvn "-Dtest=*Test" test
```

MQ 单元测试覆盖事务提交与回滚、Redis 删除顺序、JSON 消息序列化、发布确认与重试、消费 ACK、死信拒绝及实例独立队列配置。
