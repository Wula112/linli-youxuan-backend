# 秒杀请求滑动窗口限流

在 `POST /voucher-order/seckill/{id}` 的Controller方法上使用 `@RateLimiter`，通过Spring AOP执行Redis ZSet滑动窗口脚本。默认所有实例、所有优惠券共用接口额度，每个连续1秒窗口最多放行100次。

## 代码位置

位于 `hmdp-framework`，Java 包为 `com.hmdp.framework.ratelimit`；通用 Lua 随该模块打包，类路径资源名称仍是 `limiter.lua`。

```text
ratelimit/
├── RateLimiter.java         注解：key、window、limit、message、type
├── RateLimitType.java       METHOD / USER / IP
├── RateLimiterAspect.java   解析配置、构造Key、执行Lua、超限拒绝
└── RateLimitException.java  交给现有统一异常处理返回Result.fail

resources/limiter.lua        清理过期请求、检查额度、登记放行请求和设置TTL
```

限流在登录拦截器之后、秒杀业务之前执行。被拒绝时不调用秒杀Service，因此不生成订单号、不预占库存、不发布订单MQ。放行后业务返回库存不足、一人一单等失败，仍占用本次请求额度。

订单查询、模拟支付、消费建单、订单补发对账和自动关单没有添加限流注解，沿用现有流程。

## 配置

```yaml
hmdp:
  seckill:
    rate-limit:
      window: 1s
      limit: 100
```

window支持 `1s`、`500ms` 等Duration格式，limit必须为正整数。比如500ms内最多放行100次，可以设置 `window: 500ms`、`limit: 100`。值支持注解直接填写或`${...}`配置占位符，无需引入SpEL表达式解析。

初始阈值是示例，应依据压测调整，所有实例保持相同配置。项目未引入配置热更新，修改YAML后重启生效。窗口不是固定整秒计数，而是每次请求都检查最近window时间内已放行的请求。

接口上实际使用：

```java
@RateLimiter(key = "seckill", window = "${hmdp.seckill.rate-limit.window:1s}",
        limit = "${hmdp.seckill.rate-limit.limit:100}", message = "抢购人数过多，请稍后重试")
```

## Key与限流维度

Key格式：`rate:limit:{注解key}:{类名}:{方法名}({参数类型})`，USER、IP维度再追加对应标识。参数类型用于区分重载方法，参数值不参与Key，所以当前秒杀不同voucherId共享额度。

| type | 含义 |
| --- | --- |
| METHOD | 所有请求共享这个方法的额度，当前秒杀使用此方式 |
| USER | 在方法基础上按UserHolder中已登录用户ID分别计数 |
| IP | 在方法基础上按request.getRemoteAddr()分别计数 |

USER没有登录上下文时拒绝请求，不把所有人统一计入anonymous。当前秒杀入口本身已有登录拦截器。

IP模式不直接读取客户端可伪造的X-Forwarded-For等头部。位于反向代理后时，需先在可信代理边界正确配置客户端地址转发；否则remoteAddr可能是代理地址。当前METHOD模式不依赖IP。

注解依赖Spring代理调用；同一个对象内部直接调用被注解方法不会经过切面。当前外部HTTP请求调用Controller会经过代理。

## Lua处理与原文修正

1. 使用Redis TIME取得统一毫秒时间，避免不同应用服务器的时钟差异影响共享窗口。
2. 删除分数小于等于 `now-window` 的请求，保留窗口 `(now-window, now]`。
3. ZCARD达到limit直接拒绝；否则ZADD记录请求，并使用PEXPIRE设置毫秒TTL。
4. 请求member由Java生成UUID。同毫秒请求不再通过相同randomseed生成相同member，避免ZADD覆盖导致漏计数。

只记录放行请求。被拒绝的请求不会延长Key存活时间，空闲Key会自动过期；单个Key中的有效请求记录不超过配置上限。上述操作在同一次Lua中原子完成，不会出现并发先查后加突破额度的问题。

## 返回与边界

超限沿用项目现有HTTP 200 + Result.fail格式：

```json
{"success": false, "errorMsg": "抢购人数过多，请稍后重试"}
```

原前端common.js会展示Result.errorMsg，对非401的HTTP错误统一显示“服务器异常”，因此本次不改变HTTP状态约定。压测需同时检查success和errorMsg，不能只用HTTP 200统计抢购成功数。

Redis异常或脚本结果未知时不继续执行业务，沿用现有通用异常返回。该限流限制业务放行速率，不限制同时执行的请求数；请求已经到达应用，且登录Token的Redis读取发生在限流之前。

## 验证

`mvn "-Dtest=*Test" test` 包含真实AOP代理与MockMvc测试，覆盖入口拒绝、配置解析、提示返回、用户隔离、IP头部伪造、重载方法隔离和Redis故障。

`SlidingWindowRateLimiterIT` 显式设置 `HMDP_RATE_LIMIT_IT=true` 后连接专用Redis测试端口，默认6388，可用 `-Dhmdp.test.redis.port` 修改。只使用随机测试Key，覆盖双实例并发共享上限、64个并发请求独立记录、窗口滑动、500ms过期、拒绝不延长TTL和非法参数。
