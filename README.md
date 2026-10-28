# 黑马点评后端

基于 Spring Boot、MyBatis-Plus、MySQL 和 Redis 的点评项目后端，包含用户登录、商铺查询、探店笔记、关注、优惠券和秒杀订单等功能。

## 项目内容

父工程按职责拆分为四个 Maven 模块，仍作为一个 Spring Boot 应用部署：

| 模块 | 职责 |
| --- | --- |
| `hmdp-common` | 共享 DTO、常量、订单状态、用户上下文与普通工具 |
| `hmdp-framework` | 通用缓存工具、Redis 锁、限流、ID 生成、RabbitMQ 与 XXL-JOB 基础配置 |
| `hmdp-sharding` | ShardingSphere 数据源及 XA 接入 |
| `hmdp-core-service` | 启动类、接口、业务服务、实体、Mapper、业务缓存、定时任务与业务 MQ |

依赖方向为 `core-service → framework → common`、`core-service → common` 和 `core-service → sharding`。业务拓扑、消息、生产者、消费者仍集中在核心模块的 `mq/`，通用 `RabbitConfig` 位于基础组件模块。缓存放在 `cache/`，预热、对账和关单任务统一放在 `job/`。

配置、Mapper XML、业务 Lua 和数据库脚本位于 `hmdp-core-service/src/main/resources`；通用限流和锁 Lua 位于 `hmdp-framework/src/main/resources`。回归测试集中在核心模块，覆盖业务与基础组件的组合行为。

模块边界、目录和迁移说明见 [项目结构说明](docs/project-structure.md)。本仓库仅包含后端，配套前端和 Nginx 未调整。

## 开发环境

当前 `pom.xml` 配置为 Java 21、Spring Boot 4.1.1、MyBatis-Plus 3.5.17 和 Redisson 4.7.0，使用 Maven 构建。运行前需要准备 MySQL 和支持 Streams 的 Redis。

## 本地运行

1. 执行 `hmdp-core-service/src/main/resources/db/sharding-schema.sql` 创建 `hmdp_0`、`hmdp_1`。应用默认启用 `sharding` profile，采用两库两表与 XA。已有单库数据可在停写后执行 `sharding-migrate-from-hmdp.sql` 复制，保留原 ID；配置与步骤见 [分库分表说明](docs/database-sharding.md)。需要示例数据时，先在专用 `hmdp` 导入原始 `hmdp.sql`，再复制到空分片。

2. 将配置示例复制为实际配置。PowerShell 命令：

   ```powershell
   Copy-Item hmdp-core-service/src/main/resources/application.example.yaml hmdp-core-service/src/main/resources/application.yaml
   ```

   按实际环境调整数据库地址、用户名和 Redis 配置。数据库密码可通过环境变量设置：

   ```powershell
   $env:DB_PASSWORD = '你的本地数据库密码'
   ```

   `application.yaml` 已被 Git 忽略，本机配置和密码不会随源码提交。修改通用配置时，请同步更新不含凭据的 `application.example.yaml`。

3. 启动本地 Redis。当前 `RedissonConfig.java` 单独配置了 `redis://127.0.0.1:6379`；如果更换 Redis 地址或启用认证，需要同步调整该配置与 Spring Redis 配置。

4. 启动 RabbitMQ。当前秒杀异步下单与缓存失效使用 RabbitMQ，已不需要创建旧 Redis Stream 消费者组。

5. 图片上传目录在 `hmdp-common/src/main/java/com/hmdp/common/constant/SystemConstants.java` 的 `IMAGE_UPLOAD_DIR` 中定义。使用图片上传功能前，将其调整为本机图片目录，并配置静态文件服务。

6. 编译、打包并启动：

   ```shell
   mvn -DskipTests package
   java -jar hmdp-core-service/target/hm-dianping-0.0.1-SNAPSHOT.jar
   ```

   默认服务端口为 `8081`。

## 商铺缓存与布隆过滤器

商铺详情查询使用“缓存 → 布隆过滤器 → 数据库”的顺序。缓存命中直接返回；缓存未命中且过滤器判断不存在时直接返回“商铺不存在”；可能存在时查询数据库，查不到仍缓存空字符串 2 分钟，兜底处理误判和已删除的商铺。

- `ShopBloomFilter` 复用已有 Redisson，应用启动时按 ID 每批加载 1000 个已有商铺，全部加载后才开启拦截。预热失败或过滤器不可用时，继续使用原有缓存和数据库查询；运行期间过滤器丢失后，重启应用可重新全量预热。
- 新增商铺沿用 `shopService.save(shop)`，数据库生成 ID 后、事务提交前同步加入过滤器；过滤器写入失败会回滚新增。
- 新增、更新商铺成功提交事务后，立即删除该 ID 的缓存，再由独立定时线程延迟 1 秒删除一次，清理并发查询可能回写的旧值或空值。事务回滚不触发删除任务；间隔由 `RedisConstants.CACHE_SHOP_DELETE_DELAY` 配置，单位为毫秒。
- 延迟双删用于降低旧值回写的风险，延迟需要覆盖实际回源和写缓存的耗时；超时回写、Redis 故障或进程退出仍可能影响效果。当前延迟任务在本机内存中，执行失败记录错误日志，不提供持久化重试，因此不能保证严格一致性。
- 保留原有逻辑过期查询，首次缓存未命中时自动回源并写入逻辑过期缓存；空值缓存、互斥锁查询也接入同一过滤器。
- Redis Key 为 `bloom:shop`、Redisson 自动生成的配置 Key，以及预热完成标记 `bloom:shop:ready`，不设置过期时间。预计容量为 100 万、目标误判率为 1%，参数集中在 `RedisConstants` 中。
- 普通布隆过滤器不删除单个 ID，已删除商铺由空值缓存兜底。直接向数据库导入商铺后，需要重启应用补充预热。调整容量或误判率时，应停止应用、删除过滤器及其配置和完成标记后再启动，以完整重建。

## 分类与优惠券多级缓存

默认情况下，`queryTypeList()` 和 `queryVoucherOfShop(shopId)` 使用“Caffeine 本地缓存 → Redis → MySQL”的查询顺序。本地命中时直接返回；本地未命中时读取 Redis 并回填本地；两级都未命中时查数据库，再填充两级缓存。同一实例内同一个缓存 Key 使用 Caffeine 原子加载，合并并发加载请求。

| 数据 | 本地缓存 | Redis 缓存 |
| --- | --- | --- |
| 商铺分类有序列表 | 整个列表一个 Key，默认写入 5 秒后允许刷新、10 分钟硬过期 | `cache:shop-type`，正常列表默认 30 分钟过期 |
| 商铺下的优惠券列表 | 按商铺 ID 缓存，最多 1000 个商铺，默认写入 5 秒后允许刷新、10 分钟硬过期 | `cache:shop:voucher:{shopId}`，正常列表默认 10 分钟过期 |

两类列表共用泛型工具 `LocalCacheClient<K, T>`，K 为本地缓存 Key 类型，T 为列表元素类型。工具统一处理模式选择、Caffeine 容量与刷新、Redis JSON 读写、数据库回源及空列表缓存；每个业务持有独立的缓存实例。空列表也写入两级缓存，Redis 空列表仍为 2 分钟过期。

`ShopTypeCacheService` 和 `VoucherListCacheService` 只提供各自的配置、Redis Key 生成方式、列表元素类型和数据库查询函数。分类回源仍按 `sort` 升序，优惠券仍按商铺 ID 查询。例如优惠券缓存的初始化为：

```java
voucherListCache = new LocalCacheClient<>(stringRedisTemplate, cacheProperties, Voucher.class,
        shopId -> CACHE_SHOP_VOUCHER_KEY + shopId, voucherMapper::queryVoucherOfShop);
```

工具内部使用 `LoadingCache`，`@PostConstruct initCache()` 只初始化缓存容器和加载规则，首次查询时才读取 Redis 或数据库。达到刷新时间后，下次访问会触发异步刷新，同时可以返回旧值；刷新仍先查 Redis，Redis 未命中才回源 MySQL，因此 `refresh-after-write` 不代表数据库更新会在该时间内可见。

### 列表查询模式与压测

`ShopTypeCacheProperties` 和 `VoucherCacheProperties` 继承通用的 `LocalCacheProperties`，分别绑定以下配置，两类数据的模式、容量和时间可独立调整。配置示例已写入 `application.example.yaml`：

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

| mode | 分类列表与优惠券列表的查询路径 |
| --- | --- |
| `mysql` | 每次直接查询 MySQL，不读写该业务的两级缓存 |
| `redis` | Redis → MySQL，未命中时写回 Redis，绕过本地缓存 |
| `caffeine` | Caffeine → Redis → MySQL，未命中时逐级加载并回填 |

模式名不区分大小写。配置中的模式拼写错误、非正数容量或时间会在启动时报告错误，避免压测时意外使用其他路径。配置在启动时绑定，修改后重启应用。

分类列表接口为 `GET /shop-type/list`，优惠券列表接口为 `GET /voucher/list/{shopId}`。可通过启动参数同时切换两个接口的模式，无需修改代码：

```shell
java -jar hmdp-core-service/target/hm-dianping-0.0.1-SNAPSHOT.jar --hmdp.cache.shop-type.mode=mysql --hmdp.cache.voucher-list.mode=mysql
java -jar hmdp-core-service/target/hm-dianping-0.0.1-SNAPSHOT.jar --hmdp.cache.shop-type.mode=redis --hmdp.cache.voucher-list.mode=redis
java -jar hmdp-core-service/target/hm-dianping-0.0.1-SNAPSHOT.jar --hmdp.cache.shop-type.mode=caffeine --hmdp.cache.voucher-list.mode=caffeine
```

以上命令分别用于不同次启动，也可以只覆盖其中一个模式。对比热缓存吞吐量时，先用分类接口和相同商铺 ID 预热，再开始计量；冷缓存回源单独测量。`mysql` 模式仅切换对应列表的查询路径，应用其他模块、启动初始化及携带登录令牌的请求仍可能访问 Redis。

分类和优惠券列表接入 RabbitMQ 广播失效：事务提交成功后先删 Redis，再向 fanout exchange 发布消息；每个实例独立队列，消费后仅删除该实例的 Caffeine，成功手动 ACK，失败重试，耗尽后拒绝消息进入死信队列。启动前准备 RabbitMQ，无需新增数据库表或触发器。

业务 MQ 代码集中在 `com.hmdp.mq`，通用配置位于 `com.hmdp.framework.mq.config`，包括配置、失效消息、事务提交后的发布者和消费者。发布端使用 Spring AMQP 的 `JacksonJsonMessageConverter` 序列化消息，通过 `RabbitTemplate.convertAndSend()` 发送，并用 Spring 7 `RetryTemplate` 统一处理有限重试；同时保留 Confirm、Returns。`hmdp.cache.mq.enabled: false` 可关闭失效广播进行基线压测。接入方法、配置和一致性边界见 [缓存一致性说明](docs/cache-consistency.md)。

优惠券列表仍包含库存和活动时间，秒杀扣减沿用现有 Redis Lua 流程；列表库存的同步以 MySQL 已提交的库存为准。

## RabbitMQ 秒杀异步下单

秒杀请求先通过 Redis Lua 校验一人一单并预扣库存，随后发布订单消息；数据库库存扣减和订单创建由 RabbitMQ 消费者在同一事务中完成。入口返回的订单号代表请求已受理。订单使用 Direct Exchange 和共享队列竞争消费，消费者成功后 AUTO ACK，失败有限重试并进入死信队列。

缓存与订单共用 `RabbitConfig` 中的 JSON 转换器和 RabbitTemplate，拓扑分别位于 `CacheMqConfig`、`VoucherOrderMqConfig`。发布失败的预扣回退与消费共用订单锁，并按订单号校验预扣归属；重复消息不会再次扣库存。原有 Redis Stream 消费线程、pending-list、阻塞队列及同步下单历史代码和 Lua XADD 均注释保留，当前启用新增的 RabbitMQ 实现。

已有数据库需要按升级说明配置订单唯一索引；当前使用未关闭订单唯一约束，允许关闭后再次抢购。切换前先处理完旧 Stream 的积压和 pending-list。配置、升级脚本、验证方式和故障边界见 [秒杀异步下单说明](docs/voucher-order-mq.md)。

## 秒杀订单对账

秒杀预占使用“库存 String + 优惠券级 reservation Hash + 全局 pending ZSet”。Hash 同时实现一人一单与 `userId → orderId` 记录；pending 以完整 `voucherId:userId:orderId` 定位请求，预扣时由 Lua 原子登记。订单提交成功只删除 pending，保留购买资格。

`job/SeckillOrderReconcileJob` 默认每分钟核查 5 分钟前的记录，每批最多 100 条：数据库已有订单则清理索引，未建单且预占仍匹配则复用原消息有限补发。领取脚本使用 ZSCORE 与 ZADD XX 原子延期，避免多实例重复领取及清理后重新加入。默认补发最多 5 轮，最后一轮后再等待核查窗口，获取消费者同一把订单锁并重新查库，确认未建单后才原子释放预占；拿不到锁或依赖失败则保留待重试。上限为 0 时只核查，不补发、不自动释放。

配置为 `hmdp.seckill.reconcile`。旧数据补齐、Redis 持久化、单节点适用范围与验证方式见 [订单对账说明](docs/seckill-order-reconcile.md)。

## 订单支付与自动关单

订单创建后默认15分钟内允许本人模拟支付，超时由RabbitMQ固定消息TTL + DLX触发关闭，数据库扫描补齐漏通知和未完成的Redis返库。关闭后返还库存并释放本订单资格，允许原用户再次抢购，历史订单保留。MQ配置、消息、生产者和消费者仍分别归入现有mq目录。

升级前按顺序执行支付截止时间及超时关单的增量SQL，新唯一约束只限制未关闭订单。配置、模拟支付接口和测试方式见[支付说明](docs/voucher-order-payment.md)与[自动关单说明](docs/voucher-order-timeout.md)。

## XXL-JOB 调度与缓存预热

执行器与业务应用一起部署，使用 XXL-JOB 3.4.2。默认保留本地调度；以 `--spring.profiles.active=sharding,xxl` 启用后，关闭原两个本地定时入口，由 Admin 调度 Redis 预热、Caffeine 广播预热、pending 对账和关单补偿四个 Handler。

Redis 预热默认从业务库筛选正在进行、或未来10分钟内开始的上架秒杀券所属商铺，去重后每轮最多100家，无需在启动命令维护商铺ID。整轮预热成功后，将名单写入Redis供各实例装载Caffeine；没有活动时只预热分类，并发布空名单。预热仅对临时故障做最多两次指数退避重试；扫描沿用持久化状态在后续周期恢复，Admin 重试次数设为 0，避免多层重试叠加。订单事务、补发上限、延迟关单和返库规则保持原样。

需要单独准备 Admin、通信令牌及任务配置。配置示例、路由、Cron、失败处理和验证方法见 [XXL-JOB 接入说明](docs/xxl-job.md)。

## 秒杀请求限流

秒杀Controller入口使用自定义注解与Redis滑动窗口限流，默认所有实例、所有优惠券共用每秒100次的放行额度，通过 `hmdp.seckill.rate-limit.window` 和 `limit` 调整。超限沿用现有Result.fail提示，不进入库存预占和消息发布。限流模块同时支持USER和IP维度，配置与测试见[限流说明](docs/seckill-rate-limit.md)。

## 测试

```shell
mvn test
```

现有测试会连接 MySQL、Redis，并写入缓存或商铺 GEO 数据。请先完成环境配置，并使用专用测试数据；仅检查编译和打包时使用 `mvn -DskipTests package`。

验证布隆过滤器、多级缓存、消息可靠性和延迟双删的单元测试，无需启动外部服务：

```shell
mvn "-Dtest=*Test" test
```

MQ 单元测试覆盖事务提交与回滚、Redis 删除顺序、JSON 消息序列化、发布确认与重试、消费 ACK 与死信拒绝，以及实例独立队列配置。
