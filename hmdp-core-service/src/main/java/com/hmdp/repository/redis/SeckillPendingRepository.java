package com.hmdp.repository.redis;

import lombok.RequiredArgsConstructor;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

import static com.hmdp.common.constant.RedisConstants.SECKILL_PENDING_KEY;

/**
 * 秒杀待对账索引的 Redis 数据访问：扫描候选并原子领取。
 *
 * <p>只负责 pending 集合，不判断订单是否已落库；购买资格的清理与回退仍由
 * {@link SeckillReservation} 完成。
 */
@Component
@RequiredArgsConstructor
public class SeckillPendingRepository {

    private static final DefaultRedisScript<Long> CLAIM_SCRIPT = script("seckill-pending-claim.lua");

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

    private static DefaultRedisScript<Long> script(String name) {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource(name));
        script.setResultType(Long.class);
        return script;
    }
}
