-- ===== 上一版Set与Hash回退：注释保留 =====
-- -- 只回退当前订单的预扣；重复回退、旧请求回退不会影响新请求。
-- if redis.call('hget', KEYS[3], ARGV[1]) ~= ARGV[2] then
--     return 0
-- end
-- if redis.call('exists', KEYS[1]) == 0 then
--     return -1
-- end
-- redis.call('incrby', KEYS[1], 1)
-- redis.call('srem', KEYS[2], ARGV[1])
-- redis.call('hdel', KEYS[3], ARGV[1])
-- return 1
--

-- ===== 当前回退：校验归属，原子释放Hash资格、恢复库存并清理对应pending =====
-- KEYS: 库存Key、预占Hash、pending ZSet；ARGV: userId、orderId、完整pending member
-- 新增KEYS[4]：补发次数Hash，随对应pending一起清理。
if redis.call('hget', KEYS[2], ARGV[1]) ~= ARGV[2] then
    -- 只清理这笔旧请求的索引，不影响新预占。
    redis.call('zrem', KEYS[3], ARGV[3])
    redis.call('hdel', KEYS[4], ARGV[3])
    return 0
end
if redis.call('exists', KEYS[1]) == 0 then
    return -1
end
redis.call('incr', KEYS[1])
redis.call('hdel', KEYS[2], ARGV[1])
redis.call('zrem', KEYS[3], ARGV[3])
redis.call('hdel', KEYS[4], ARGV[3])
return 1
