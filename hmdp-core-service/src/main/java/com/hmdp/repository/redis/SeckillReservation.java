package com.hmdp.repository.redis;

import lombok.RequiredArgsConstructor;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.List;

import static com.hmdp.common.constant.RedisConstants.SECKILL_PENDING_KEY;
import static com.hmdp.common.constant.RedisConstants.SECKILL_PENDING_RETRIES_KEY;
import static com.hmdp.common.constant.RedisConstants.SECKILL_RESERVATION_KEY;
import static com.hmdp.common.constant.RedisConstants.SECKILL_RELEASED_KEY;
import static com.hmdp.common.constant.RedisConstants.SECKILL_STOCK_KEY;
import static com.hmdp.common.constant.RedisConstants.SECKILL_TXN_KEY;
import static com.hmdp.common.constant.RedisConstants.SECKILL_REQUESTS_KEY;
import static com.hmdp.common.constant.RedisConstants.SECKILL_CIRCUIT_KEY;

/**
 * 秒杀预占的 Redis 数据访问：预扣、归属查询、回退、pending 清理及事务标记查询。
 */
@Component
@RequiredArgsConstructor
public class SeckillReservation {

    /** 预扣成功。 */
    public static final long SUCCESS = 0;
    /** 库存不足。 */
    public static final long STOCK_EMPTY = 1;
    /** 同一用户重复下单。 */
    public static final long DUPLICATE = 2;
    /** 熔断期间拒绝新预占，已受理请求仍继续建单。 */
    public static final long CIRCUIT_OPEN = 3;
    /** 本地事务没有给出确定结果，等待 Broker 回查。 */
    public static final long UNKNOWN = -1;
    /** 事务标记保留时长，覆盖 Broker 的回查窗口即可。 */
    private static final long TXN_MARKER_TTL_SECONDS = 3600;

    private static final DefaultRedisScript<Long> RESERVE_SCRIPT = script("seckill.lua");
    private static final DefaultRedisScript<Long> ROLLBACK_SCRIPT = script("seckill-rollback.lua");
    private static final DefaultRedisScript<Long> PENDING_CLEAR_SCRIPT = script("seckill-pending-clear.lua");

    private final StringRedisTemplate stringRedisTemplate;

    /** 扣库存、写预占、登记待对账索引并写事务标记。 */
    public long reserve(Long voucherId, Long userId, Long orderId) {
        Long result = stringRedisTemplate.execute(RESERVE_SCRIPT,
                List.of(SECKILL_STOCK_KEY + voucherId, SECKILL_RESERVATION_KEY + voucherId,
                        SECKILL_PENDING_KEY, SECKILL_TXN_KEY + orderId, SECKILL_REQUESTS_KEY, SECKILL_CIRCUIT_KEY),
                userId.toString(), orderId.toString(), voucherId.toString(),
                Long.toString(System.currentTimeMillis()), Long.toString(TXN_MARKER_TTL_SECONDS));
        if (result == null) {
            throw new IllegalStateException("秒杀预扣结果为空，orderId=" + orderId);
        }
        return result;
    }

    /** 初始化秒杀库存，与预扣、回退脚本使用同一个库存 Key。 */
    public void initStock(Long voucherId, Integer stock) {
        stringRedisTemplate.opsForValue().set(SECKILL_STOCK_KEY + voucherId, String.valueOf(stock));
    }

    /** 查询本地事务标记是否存在。 */
    public boolean hasTransactionMarker(long orderId) {
        return Boolean.TRUE.equals(stringRedisTemplate.hasKey(SECKILL_TXN_KEY + orderId));
    }

    /** 查询当前用户的预占订单号。 */
    public Object getReservedOrderId(Long voucherId, Long userId) {
        return stringRedisTemplate.opsForHash().get(SECKILL_RESERVATION_KEY + voucherId, userId.toString());
    }

    /** 按订单号查找尚未建单的请求；成功建单后由提交回调清理。 */
    public String findAcceptedRequestMember(Long orderId) {
        Object value = stringRedisTemplate.opsForHash().get(SECKILL_REQUESTS_KEY, orderId.toString());
        if (value == null) return null;
        String member = value.toString();
        if (!member.endsWith(":" + orderId)) throw new IllegalStateException("订单请求索引不匹配，orderId=" + orderId);
        return member;
    }

    public boolean hasReleaseReceipt(Long orderId) {
        return Boolean.TRUE.equals(stringRedisTemplate.hasKey(SECKILL_RELEASED_KEY + orderId));
    }

    /** 1=本次释放，2=此前已释放，0=归属缺失或不匹配，负数=数据异常。 */
    public Long rollback(Long voucherId, Long userId, Long orderId) {
        // 沿用原调用链对订单、用户和优惠券标识的校验。
        Assert.isTrue(orderId != null && orderId > 0, "orderId必须大于0");
        Assert.isTrue(userId != null && userId > 0, "userId必须大于0");
        Assert.isTrue(voucherId != null && voucherId > 0, "voucherId必须大于0");
        return stringRedisTemplate.execute(ROLLBACK_SCRIPT,
                List.of(SECKILL_STOCK_KEY + voucherId, SECKILL_RESERVATION_KEY + voucherId,
                        SECKILL_PENDING_KEY, SECKILL_PENDING_RETRIES_KEY, SECKILL_RELEASED_KEY + orderId,
                        SECKILL_REQUESTS_KEY),
                userId.toString(), orderId.toString(), voucherId + ":" + userId + ":" + orderId);
    }

    /** 清理已完成订单的待对账索引；失败由对账任务重试。 */
    public void clearPending(String member) {
        int separator = member.lastIndexOf(':');
        Assert.isTrue(separator > 0 && separator < member.length() - 1, "待对账成员缺少订单号");
        String orderId = member.substring(separator + 1);
        stringRedisTemplate.execute(PENDING_CLEAR_SCRIPT,
                List.of(SECKILL_PENDING_KEY, SECKILL_PENDING_RETRIES_KEY, SECKILL_REQUESTS_KEY),
                member, orderId);
    }

    private static DefaultRedisScript<Long> script(String name) {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource(name));
        script.setResultType(Long.class);
        return script;
    }
}
