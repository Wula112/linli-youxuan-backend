package com.hmdp.repository.redis;

import lombok.RequiredArgsConstructor;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

import static com.hmdp.common.constant.RedisConstants.SECKILL_PENDING_KEY;
import static com.hmdp.common.constant.RedisConstants.SECKILL_PENDING_RETRIES_KEY;
import static com.hmdp.common.constant.RedisConstants.SECKILL_RESERVATION_KEY;

/**
 * 秒杀待对账索引的 Redis 数据访问：扫描候选、原子领取与补发计数。
 *
 * <p>只负责 pending 集合和补发计数，不判断订单是否已落库；购买资格的清理与回退仍由
 * {@link SeckillReservation} 完成。
 */
@Component
@RequiredArgsConstructor
public class SeckillPendingRepository {

    private static final DefaultRedisScript<Long> CLAIM_SCRIPT = script("seckill-pending-claim.lua");
    private static final DefaultRedisScript<Long> RETRY_SCRIPT = script("seckill-pending-retry.lua");

    private final StringRedisTemplate stringRedisTemplate;

    /** 读取到期未确认的成员，按分数升序；超过截止时间的记录留给下一轮。 */
    public Set<String> findDue(long deadlineMillis, int limit) {
        return stringRedisTemplate.opsForZSet()
                .rangeByScore(SECKILL_PENDING_KEY, 0, deadlineMillis, 0, limit);
    }

    /** 原子领取并延期；返回 false 表示已被其他实例领取或清理。 */
    public boolean claim(String member, long deadlineMillis) {
        Long claimed = stringRedisTemplate.execute(CLAIM_SCRIPT, List.of(SECKILL_PENDING_KEY),
                member, Long.toString(deadlineMillis), Long.toString(System.currentTimeMillis()));
        if (claimed == null) {
            throw new IllegalStateException("对账领取结果为空，member=" + member);
        }
        return claimed == 1L;
    }

    /**
     * 原子累计补发次数，超出上限不再占用额度。
     *
     * @return 小于0表示已达上限或补发已禁用；0表示预占已失效、不应再发；其余为本次补发轮次
     */
    public long bumpRetry(String member, Long voucherId, Long userId, Long orderId, int maxRetries) {
        Long retryCount = stringRedisTemplate.execute(RETRY_SCRIPT,
                List.of(SECKILL_PENDING_KEY, SECKILL_PENDING_RETRIES_KEY,
                        SECKILL_RESERVATION_KEY + voucherId),
                member, Integer.toString(maxRetries), userId.toString(), orderId.toString());
        if (retryCount == null) {
            throw new IllegalStateException("秒杀订单补发计数结果为空，member=" + member);
        }
        return retryCount;
    }

    private static DefaultRedisScript<Long> script(String name) {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource(name));
        script.setResultType(Long.class);
        return script;
    }
}
