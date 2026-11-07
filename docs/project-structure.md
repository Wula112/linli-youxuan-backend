# 项目模块与目录

按照 [Spring Boot 代码组织建议](https://docs.spring.io/spring-boot/reference/using/structuring-your-code.html)，启动类保留在 `com.hmdp` 根包，其他组件位于其子包，组件扫描范围因此保持稳定。核心模块按入口、业务、数据访问和消息适配划分职责，不额外引入只转发调用的接口或模块。

```text
hm-dianping/
├── pom.xml                         聚合构建、依赖版本管理
├── hmdp-common/
│   └── src/main/java/com/hmdp/common/
│       ├── constant/               Redis Key、系统常量
│       ├── context/                UserHolder
│       ├── dto/                    Result、UserDTO 等共享数据结构
│       ├── enums/                  VoucherOrderStatus
│       └── util/                   正则校验、密码工具
├── hmdp-framework/
│   ├── src/main/java/com/hmdp/framework/
│   │   ├── cache/                  CacheClient、LocalCacheClient、RedisData
│   │   │   └── config/             LocalCacheProperties
│   │   ├── id/                     ID 工具及生成器配置
│   │   ├── job/config/             XXL-JOB 执行器与连接参数
│   │   ├── lock/                   Redis 锁及 Redisson 配置
│   │   └── ratelimit/              注解、切面、维度与限流异常
│   └── src/main/resources/         limiter.lua、UnLock.lua
├── hmdp-sharding/
│   └── src/main/java/com/hmdp/sharding/config/
│       └── ShardingDataSourceConfig.java
├── hmdp-core-service/
│   ├── src/main/java/com/hmdp/
│   │   ├── HmDianPingApplication.java
│   │   ├── controller/             HTTP 入口
│   │   │   └── advice/             HTTP 异常处理
│   │   ├── service/impl/           业务流程、事务与订单状态处理（支付、关单为订单门面的协作类）
│   │   ├── entity/                 数据库实体
│   │   ├── mapper/                 MyBatis 数据访问
│   │   ├── repository/redis/       秒杀预占、回退、事务标记与 pending 对账的 Redis 访问
│   │   ├── cache/                  分类、优惠券缓存、商铺布隆过滤器与展示数据预热
│   │   │   └── config/             业务缓存参数、延迟双删线程池
│   │   ├── job/                    预热、pending 对账、超时关单与返库补偿扫描
│   │   │   ├── config/             调度模式、任务参数与本地调度入口
│   │   │   └── support/            单轮计数、预算与结果上报
│   │   ├── mq/
│   │   │   ├── config/             RocketMQ 客户端装配、主题常量与参数
│   │   │   ├── message/            业务消息
│   │   │   ├── support/            消息体编解码
│   │   │   ├── producer/           消息发布、事务上下文及回查
│   │   │   └── consumer/           业务消费入口
│   │   ├── interceptor/           登录校验与令牌刷新
│   │   └── config/                Web、MyBatis、JSON 和订单参数
│   ├── src/main/resources/         应用配置、分片规则、SQL、Mapper、秒杀 Lua
│   └── src/test/java/              应用及跨模块回归测试
└── docs/
```

## 依赖边界

- `hmdp-common` 不依赖其余三个模块，也不声明数据源、MQ 或 Redis 连接。
- `hmdp-framework` 只依赖 `hmdp-common`，不引用业务实体、Mapper、Service。泛型缓存通过调用方提供的函数回源。用户维度限流仍读取同一个 `UserHolder`。
- `hmdp-sharding` 封装数据源和 XA 接入，不引用订单 Service。具体分片规则、数据源参数与 SQL 属于当前应用，留在核心模块的资源目录。
- `hmdp-core-service` 依赖以上模块，负责业务组合与启动。业务缓存知道如何查分类或优惠券，业务 MQ 知道如何失效缓存、创建订单和关闭订单，因此这些类留在核心模块。

公共模块只产出普通 jar，Spring Boot 打包插件只用于核心模块。现有 `com.hmdp.HmDianPingApplication` 继续扫描 `com.hmdp`，无需额外的自动配置框架。MyBatis 的 `com.hmdp.mapper` 扫描与实体别名包保持不变。

核心模块中，HTTP 请求由 `controller` 进入 `service`；建单、模拟支付与超时关单分别委托给独立服务。建单、死信登记、对账和关单共用 `VoucherOrderProcessService` 的数据库处理记录及事务仲裁；`VoucherOrderCompensationService` 只回补已关单订单，`SeckillCircuitBreaker` 保存死信熔断状态。`SeckillReservation` 封装预占、释放凭证和 pending 清理，`SeckillPendingRepository` 只负责 pending 扫描与领取。MQ 消费者处理下单、死信和关单消息；`job` 恢复建单并处理关单返库，不再补发订单消息。

Redis 预占与回退接口接收优惠券、用户和订单标识，不依赖 MQ 消息类；MQ 消息格式仍由 `mq.message` 管理。滚动分页结果使用公共 DTO，Web 异常处理归入 HTTP 层。测试位于核心模块，并尽量与被测组件的包位置对应；基础组件不依赖业务模块。

## 本次迁移的范围

模块拆分只改变 Maven 归属、包位置、引用及构建文档。原注释以及注释保留的 Redis Stream、阻塞队列和同步下单实现继续保留。后续 XXL-JOB 接入的变更范围单独记录在 [任务调度说明](xxl-job.md)，订单事务、Lua 和 SQL 沿用原实现。

`com.hmdp.mq.message` 中的消息类名保持不变，消息体仍按字段名序列化，历史积压消息可继续反序列化。Controller 包名保持不变，基于方法名生成的限流 Key 不变。Redis Key 和数据库字段不改名，不需要迁移已有数据。

原 `reconcile` 中的任务与配置合并到 `job`、`job.config`。默认仍由 Spring `@Scheduled` 触发；`xxl` 模式停用本地调度，由四个 `@XxlJob` 入口调用业务批处理。执行器的基础配置放在 framework，业务 Handler 留在 core-service，不额外拆分任务微服务。

## 构建与启动

在含聚合 `pom.xml` 的 `hm-dianping/` 目录执行：

```shell
mvn "-Dtest=*Test" test
mvn -DskipTests package
java -jar hmdp-core-service/target/hm-dianping-0.0.1-SNAPSHOT.jar
```

集成测试的环境开关和专用端口沿用原文档。从父工程执行指定测试，会自动构建依赖模块；没有匹配测试的公共模块会跳过测试。

本机 `application.yaml` 已随资源迁移到 `hmdp-core-service/src/main/resources`，仍被 Git 忽略。新环境从同目录的 `application.example.yaml` 复制配置。外部配置文件仍可通过 Spring Boot 原参数指定。

IDEA 重新加载根 `pom.xml`，使用 `hmdp-core-service` 的 classpath 启动 `com.hmdp.HmDianPingApplication`。仍从仓库根目录运行时，已有相对路径（例如 XA 日志目录）的含义不变；修改运行配置时应保留原工作目录。
