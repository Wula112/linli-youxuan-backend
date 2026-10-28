package com.hmdp.job;

import com.hmdp.cache.CacheWarmupService;
import com.hmdp.cache.config.CacheWarmupProperties;
import com.hmdp.job.config.JobProperties;
import com.hmdp.job.support.JobRun;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.core.retry.RetryTemplate;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.stereotype.Component;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.sql.SQLTransientException;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.concurrent.TimeoutException;

@Slf4j
@Component
@ConditionalOnProperty(prefix = "hmdp.cache.warmup", name = "enabled", havingValue = "true")
public class CacheWarmupJob implements ApplicationRunner {
    private final CacheWarmupService service;
    private final CacheWarmupProperties properties;
    private final JobProperties jobProperties;

    public CacheWarmupJob(CacheWarmupService service, CacheWarmupProperties properties, JobProperties jobProperties) {
        this.service = service;
        this.properties = properties;
        this.jobProperties = jobProperties;
    }

    @XxlJob("hotCacheRedisWarmup")
    public void warmRedisByXxl() { warmRedis().reportToXxl(); }

    @XxlJob("hotCacheLocalWarmup")
    public void warmLocalByXxl() { warmLocal().reportToXxl(); }

    public JobRun warmRedis() {
        JobRun run = new JobRun(jobProperties.getMaxRunTime());
        RetryTemplate retry = createRetry(run);
        executeOne("商铺分类", service::warmShopTypesRedis, retry, run);
        List<Long> shopIds = executeStep("筛选预热商铺", service::findShopIdsToWarm, retry, run);
        if (shopIds == null) return finish(run);

        for (Long shopId : shopIds) {
            if (!run.canContinue()) break;
            // 预热商铺数据（详情和优惠券列表）
            executeOne("商铺:" + shopId, () -> service.warmShopRedis(shopId), retry, run);
            executeOne("优惠券列表:" + shopId, () -> service.warmVouchersRedis(shopId), retry, run);
        }
        // 全部数据准备好后才发布名单；部分失败或超时保留旧名单，不触发本地子任务。
        if (run.successful() && run.canContinue()) {
            executeOne("发布预热商铺名单", () -> service.publishShopIds(shopIds), retry, run);
        }
        return finish(run);
    }

    public JobRun warmLocal() {
        JobRun run = new JobRun(jobProperties.getMaxRunTime());
        RetryTemplate retry = createRetry(run);
        executeOne("商铺分类", service::warmShopTypesLocal, retry, run);
        List<Long> shopIds = executeStep("读取预热商铺名单", service::loadPublishedShopIds, retry, run);
        if (shopIds == null) return finish(run);

        // 本地任务使用广播，每个实例装载完整名单，不按分片序号拆分。
        // 原实现使用配置名单 properties.getHotShopIds()，现在统一读取最新成功批次的Redis名单。
        for (Long shopId : shopIds) {
            if (!run.canContinue()) break;
            executeOne("优惠券列表:" + shopId, () -> service.warmVouchersLocal(shopId), retry, run);
        }
        return finish(run);
    }

    private RetryTemplate createRetry(JobRun run) {
        return new RetryTemplate(RetryPolicy.builder()
                .maxRetries(properties.getMaxRetries())
                .delay(properties.getRetryDelay()).multiplier(2).maxDelay(properties.getMaxRetryDelay())
                .predicate(ex -> run.canContinue() && transientFailure(ex)).build());
    }

    private JobRun finish(JobRun run) {
        // 预热必须完成整份名单才算成功，避免部分成功触发下游本地预热。
        if (run.isLimited()) run.failed();
        return run;
    }

    private void executeOne(String item, Runnable action, RetryTemplate retry, JobRun run) {
        executeStep(item, () -> {
            action.run();
            return Boolean.TRUE;
        }, retry, run);
    }

    /** 失败统一计数并保留其他项的处理机会；返回null表示未执行或执行失败。 */
    private <T> T executeStep(String item, Supplier<T> action, RetryTemplate retry, JobRun run) {
        if (!run.canContinue()) return null;
        run.checked();
        try {
            return retry.execute(() -> {
                if (!run.canContinue()) throw new IllegalStateException("预热预算耗尽或任务已中断");
                return Objects.requireNonNull(action.get(), "预热步骤返回空结果：" + item);
            });
        } catch (Exception ex) {
            run.failed();
            log.error("缓存预热失败，item={}", item, ex);
            XxlJobHelper.log("缓存预热失败，item={}，原因={}", item, ex.toString());
            if (hasInterruptedCause(ex)) Thread.currentThread().interrupt();
            run.canContinue();
            return null;
        }
    }

    /**
     * 判断异常是否属于「瞬时故障」，只有瞬时故障才会触发重试。
     * @param error 待判断的异常（通常为 retry predicate 传入的异常）
     * @return true 表示属于瞬时故障，可以重试；false 表示非瞬时或已中断，不应重试
     */
    static boolean transientFailure(Throwable error) {
        // 中断异常代表外部取消信号，不属于瞬时故障，直接放弃重试
        if (hasInterruptedCause(error)) return false;
        // 沿 cause 链向上遍历，直到根异常；若某节点的 cause 指向自身则终止，防止循环引用导致死循环
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof RedisConnectionFailureException || cause instanceof TransientDataAccessException
                    || cause instanceof RecoverableDataAccessException || cause instanceof SQLTransientException
                    || cause instanceof SocketTimeoutException || cause instanceof ConnectException
                    || cause instanceof TimeoutException) return true;
            // 防御性中断：当异常的 cause 指向自身时，说明已到达链尾，跳出避免死循环
            if (cause == cause.getCause()) break;
        }
        return false;
    }

    private static boolean hasInterruptedCause(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof InterruptedException) return true;
            if (cause == cause.getCause()) break;
        }
        return Thread.currentThread().isInterrupted();
    }

    @Override
    public void run(ApplicationArguments args) {
        if (properties.isStartupLocal()) {
            // 新实例仅从Redis装载L1；失败不阻断启动，正常请求仍走原查询链路。
            log.info("启动时本地缓存预热：{}", warmLocal());
        }
    }
}
