# XXL-JOB：缓存预热、订单对账与关单补偿

执行器复用现有 Spring Boot 应用，接入 `xxl-job-core 3.4.2`，不另建任务服务。Admin 需要独立部署并使用相同版本。RocketMQ 定时消息仍负责及时通知，定时扫描继续处理漏通知和 Redis 返库未完成的订单。

## 代码组织

```text
hmdp-framework/.../framework/job/config/
    XxlJobConfig                     执行器Bean，连接、注册、日志配置
    XxlJobProperties                 xxl.job配置
hmdp-core-service/.../cache/
    CacheWarmupService               商铺、分类和优惠券展示数据的预热
    config/CacheWarmupProperties     活动提前量、商铺数量上限、TTL与重试参数
hmdp-core-service/.../job/
    CacheWarmupJob                   Redis预热、本地预热两个Handler
    SeckillOrderReconcileJob         原pending核查与有限补发
    OrderTimeoutJob                  原关单、返库补偿扫描
    config/JobProperties             local/xxl选择、单轮时间预算
    config/SeckillReconcileConfig    仅local模式注册原本地定时入口
    config/OrderTimeoutConfig        仅local模式注册原本地定时入口
    support/JobRun                   单轮计数、预算与执行结果上报
```

业务任务不继承抽象Job基类，不引入任务表或自定义任务队列。旧调度注解保留为注释，原关单扫描实现也注释保留；订单业务方法、事务、Lua、原 Redis Stream 历史代码保持原样。

## 启用

默认仍为 `hmdp.job.scheduler=local`，不启动 XXL-JOB 执行器。已有本机配置无需改动。启用时读取新增的 `application-xxl.yaml`：

```powershell
$env:XXL_JOB_ADMIN_ADDRESSES = 'http://127.0.0.1:8080'
$env:XXL_JOB_ACCESS_TOKEN = '填写与Admin一致的令牌'
$env:XXL_JOB_EXECUTOR_IP = '填写Admin可以访问的本机IP'
$env:XXL_JOB_EXECUTOR_PORT = '9999'
mvn -DskipTests package
java -jar hmdp-core-service/target/hm-dianping-0.0.1-SNAPSHOT.jar --spring.profiles.active=sharding,xxl
```

无需在启动参数中维护商铺ID。Redis任务从业务库自动筛选活动商铺；Caffeine任务和新实例启动预热读取Redis中最新成功批次的名单。默认提前10分钟、每轮最多100家，具体规则见下文。

注意保留 `sharding` profile；本项目默认启用分片，但显式设置 `spring.profiles.active` 会替换默认 profile。`xxl.job.admin-addresses` 支持逗号分隔多个调度中心地址。3.4.2 默认 Admin 根路径与文章的旧版 `/xxl-job-admin` 示例不同，应填写实际部署地址。配置在启动时绑定，不支持未经实现的配置热更新。

Admin 与执行器必须双向可达：执行器连接 Admin，Admin 连接执行器 IP/端口。经 NAT 转发时可用 `XXL_JOB_EXECUTOR_ADDRESS` 设置完整注册地址；同机多实例设置不同执行器端口和日志目录。

在 Admin 的执行器管理中新增 `AppName=hmdp-job-executor`，选择自动注册。按官方同版本 SQL 初始化 Admin 自己的调度数据库；无需修改 HMDP 业务表。本次只提供执行器接入，不在业务应用中自动创建 Admin 数据库或写入任务配置。

## 四个任务的后台配置

运行模式全部使用 `BEAN`，阻塞策略使用 **丢弃后续调度（DISCARD_LATER）**，过期调度策略使用 **忽略（DO_NOTHING）**，失败重试次数全部填 **0**，任务超时建议 **60秒**。这里忽略的是重复调度机会，原订单、pending和补偿状态仍保存在数据库或Redis，后续轮次继续处理。

| JobHandler | 调度 | 路由 | 职责 |
| --- | --- | --- | --- |
| `hotCacheRedisWarmup` | Cron `0 * * * * ?`，活动前也可手动触发 | 故障转移 `FAILOVER` | 一台实例准备共享Redis展示数据 |
| `hotCacheLocalWarmup` | 无独立周期，由前一个任务成功触发子任务 | 分片广播 `SHARDING_BROADCAST` | 所有实例装载自己的Caffeine |
| `seckillOrderReconcile` | Cron `0 * * * * ?` | `FAILOVER` | 每分钟核查超时pending |
| `orderTimeoutCompensate` | Cron `0 * * * * ?` | `FAILOVER` | 每分钟扫描超时未支付订单与待返库记录 |

先创建本地预热任务，再将其任务ID填入 Redis 预热任务的“子任务ID”。子任务从Redis读取已发布名单，不依赖父任务参数自动传递，也不重新查库筛选。每个 Handler 只建一个任务定义；不要额外创建多个不同任务ID来并行调度同一个扫描器。各实例的筛选规则保持一致，确保父任务切换执行器后口径不变。

Redis 预热建议每分钟检查一次，剩余有效期不超过2分钟才回源。周期应短于这个阈值并留出执行时间；若周期设为5分钟而阈值仍为2分钟，可能跳过刷新窗口，直到缓存已经过期才处理。任务失败或调度延迟时，正常请求仍可按原链路回源。

本地预热虽然选“分片广播”，但每个实例都装载完整名单，不按 shardIndex 切割。否则每台机器的 Caffeine 只预热了部分数据。新实例启动时也从 Redis 补充自己的 L1；失败记录日志，正常请求仍沿用原查询链路。可用 `hmdp.cache.warmup.startup-local=false` 关闭启动预热。

切换时让所有实例统一使用 `xxl`，再启用 Admin 任务。`local` 和 `xxl` 是每个实例上的互斥模式，不是自动故障切换：Admin 暂停期间应用不会偷偷启用本地调度。需要回退时先停 Admin 任务，再统一切回 `local`。

## 失败重试的边界

采用文章的“临时故障才重试、指数退避、有次数上限”，不直接照搬文章的框架API和重试层数。

| 操作 | 本次重试方式 | 耗尽或失败后的处理 |
| --- | --- | --- |
| 单项缓存预热 | Spring 7 RetryTemplate；默认首次+2次重试，等待1秒、2秒，间隔上限5秒 | 继续处理其他热点，整轮标记失败；下一轮再处理，失败轮次不触发子任务 |
| pending核查 | 不对整批立即重试；继续使用Lua领取后的等待窗口 | 保留原pending、订单号及已累计补发次数，下次超时核查继续 |
| 发布订单消息 | 原Publisher有限重试保留 | 仍受每笔订单最多5轮对账补发限制 |
| 关单、返库补偿 | 当前批次继续处理其他订单；下个扫描周期再核查失败记录 | 数据库状态不丢失，条件更新与Lua继续保证重复执行不重复返库 |

预热仅重试 Redis 连接故障、Spring 临时/可恢复数据访问异常、SQL临时异常、连接或网络超时及其包装异常。参数错误、数据解析失败、L2数据缺失等不立即重试。商铺不存在则按原规则缓存空值，是正常查询结果。中断不重试，保留线程中断标记并停止领取新记录。

`max-retries=2` 是两次额外尝试，总执行次数最多三次。上限可配置为0到3；不会在最后一次失败后再无意义地睡眠。重试仅覆盖当前缓存项，不重跑已成功的整份名单。Admin 的重试次数保持0，避免“调度重试×缓存重试×MQ重试”叠加，也避免把业务错误重新整批执行。

订单补发次数与调度执行次数是不同计数。任务失败、重新触发或切换执行器都不会重置 `seckill:pending:retries`。最后一轮补发后仍等待原超时窗口；下一次领取后持同一把订单锁重新查库，只有仍未建单且预占归属匹配，才按原流程释放预占。

原生 XXL-JOB 支持配置失败重试次数，但不应把它理解为可依赖的“精确固定10秒退避”。失败检查周期、调度队列与执行器状态都会影响实际时刻。本实现明确由下一轮扫描或小范围 RetryTemplate 承担重试，不依赖这个时间假设。

## 自动筛选与名单传递

`VoucherMapper.findWarmupShopIds` 关联优惠券与秒杀信息，条件是 `status=1`（上架）、`type=1`（秒杀）、`end_time > now`、`begin_time <= now + ahead-of-start`，并排除开始时间不早于结束时间的无效活动。正在进行的活动也包含在内，不按剩余库存过滤，因为售罄信息仍然需要展示。

查询只返回商铺ID，按商铺去重、按ID升序后限制数量。两张表是绑定分片，ShardingSphere负责跨分片合并。`max-shops` 是每轮候选上限，不是分页大小；超出上限的商铺仍走正常查询链路，当前不额外引入轮换扫描或访问热度统计。这个筛选表达活动预热规则，不声称是实时访问量排行榜。

```yaml
hmdp:
  cache:
    warmup:
      ahead-of-start: 10m   # 提前量，0表示只选择已开始的活动
      max-shops: 100       # 自动筛选最多100家，可设置1到1000
      shop-list-ttl: 5m    # 成功名单有效期，需长于正常调度间隔
      hot-shop-ids: []     # 默认自动筛选；仅压测/临时指定时填写，非空会覆盖筛选
```

流程为：预热分类 → 筛选活动商铺 → 预热商铺详情和优惠券列表 → 全部成功后，用单次带TTL的SET发布 `cache:warmup:shop-ids` → 本地广播从这份JSON名单装载各自Caffeine。分类独立处理，即使没有活动商铺也会预热。

筛选、数据预热、名单发布均使用现有有限重试策略。筛选或数据预热失败、预算耗尽、中断时不发布新名单，本轮标记失败，Admin不触发子任务；之前已成功写入的展示缓存不回滚。名单发布失败也上报失败，下一轮重试。

没有活动时仍发布 `[]`，使上一次活动名单退出预热范围；不主动删除其展示缓存，继续由原失效机制和TTL管理。名单默认5分钟后过期，避免调度长期停用后继续使用陈旧名单。本地预热遇到名单缺失或数据缺失会失败，不回源数据库；启动时遇到这种情况只记录日志，不阻断业务启动。广播读取最新成功名单，不维护每轮历史版本。

## 预热范围与一致性

- 分类和优惠券列表仍使用现有 JSON、Redis Key、TTL及空列表短期缓存。Redis 剩余TTL充足时跳过回源；需要刷新时先查询再覆盖，不先删除整个缓存。
- 商铺详情使用原查询端支持的 `RedisData(data, expireTime)` 格式，检查逻辑过期时间，不给这类无物理TTL的Key误判永久有效。
- 本地预热绕过旧L1，从L2取值后放入当前Caffeine。L2缺失时本轮报失败，保留已有L1，不让所有实例同时回源MySQL。
- `mysql` 模式不预热两级列表缓存，`redis` 模式只预热L2，`caffeine` 模式两级都可预热。
- 任务只处理展示缓存，绝不初始化或覆盖 `seckill:stock:*`、`seckill:reservation:*`、`seckill:pending`。

预热不建立强一致性协议。并发更新仍沿用已有失效消息、延迟双删和TTL机制；不会因为执行了预热就保证数据库与展示缓存实时一致。

## 执行预算、游标与失败可见性

`hmdp.job.max-run-time` 默认30秒，是软预算：到时不再领取新记录，不强行停止正在执行的SQL或事务。关单和返库两类扫描各使用至多一半预算，每类仍最多10批；pending仍最多100条。超出软预算的在途I/O可能继续执行，连接池、数据库和Redis客户端仍需配置合理超时；Admin的60秒中断也不等同于线程已经终止。

`job:order-timeout:cursor` Hash 保存 `unpaid`、`restore` 两个游标；到扫描末尾归零。游标不是业务成功标记，失败订单仍保留在原表，下轮遍历会再次命中。游标丢失只会重扫，不会改变订单结果。`lock:job:order-timeout` 保护游标的领取与推进，避免不同执行器同时覆盖进度；最终业务幂等依旧由原SQL与Lua保证。

单条失败不会阻断后续记录，但会累计失败数。任务结束用 `handleFail()` 或 `handleSuccess()`明确上报检查、跳过、失败数量；详细异常写入应用日志及XXL任务日志。空批次、领取竞争失败、订单锁忙是正常跳过；预占缺失、标识冲突、SQL或Redis错误会报失败，不能捕获后无条件显示成功。

扫描达到预算、保存进度后可正常结束；预热必须完成整份名单才算成功，未完成不能触发下游预热。保留 Admin 自带的失败日志与告警配置入口，本次不引入飞书、短信、Nacos、额外线程池或通知框架。

## 验证

```shell
mvn "-Dtest=*Test" test
```

覆盖本地/XXL互斥、功能开关、错误配置、临时异常重试上限、业务错误不重试、完整广播名单、中断退出、软预算、跨实例游标恢复及部分失败上报。活动预热还覆盖空名单替换、部分失败不发布、名单TTL和雪花ID精度；`ShardingXaIT` 在隔离MySQL上验证活动时间边界、上下架过滤、跨分片去重及全局数量上限。

专用执行器联调不需要业务Redis或MySQL，通过临时本机端口运行真实XXL执行器，HTTP触发四个Handler，用本机Admin桩验证注册与成功/失败回调：

```powershell
$env:HMDP_XXL_JOB_IT = 'true'
mvn '-Dtest=XxlJobExecutorIT' test
```

原订单流程集成测试仍按既有专用测试端口和环境开关运行，验证补发耗尽释放、迟到消息、数据库提交竞争、支付与关单返库。生产Admin的实际注册、路由和Cron仍需按上表配置后验收。

参考：[用户提供的文章](https://zhuanlan.zhihu.com/p/2007560773665317568)（依据用户粘贴正文）、[XXL-JOB官方文档](https://github.com/xuxueli/xxl-job/blob/3.4.2/doc/XXL-JOB官方文档.md)、[3.4.2执行器源码](https://github.com/xuxueli/xxl-job/blob/3.4.2/xxl-job-core/src/main/java/com/xxl/job/core/executor/XxlJobExecutor.java)。
