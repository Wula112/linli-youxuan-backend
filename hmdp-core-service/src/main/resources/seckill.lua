-- ===== 原有 Redis Stream Lua：完整注释保留，当前不执行 =====
-- -- 1. 优惠券ID
-- local voucherId = ARGV[1]
-- -- 2. 用户ID
-- local userId = ARGV[2]
-- -- 3. 订单ID
-- local orderId = ARGV[3]
--
-- --库存Key
-- local stockKey = 'seckill:stock:' .. voucherId
-- --优惠券订单Key
-- local orderKey = 'seckill:order:' .. voucherId
--
-- -- 3. 查询库存是否充足
-- if(tonumber(redis.call('get', stockKey)) <= 0) then
--     -- 3.1 库存不足，返回1
--     return 1
-- end
--
-- -- 3.2 校验一人一单
-- if(redis.call('sismember', orderKey, userId) == 1) then
--     -- 3.3 购买过，返回2
--     return 2
-- end
-- -- 预扣库存
-- redis.call('incrby', stockKey, -1)
-- redis.call('sadd', orderKey, userId)
-- -- 将订单信息加入消息队列 XADD key ID|* field value [field value ...]
-- redis.call('xadd','stream.orders','*','userId', userId, 'voucherId', voucherId, 'id', orderId)
-- return 0
--

-- ===== 上一版RabbitMQ预扣：注释保留 =====
-- -- ===== 新增 RabbitMQ 下单的 Redis 校验与预扣逻辑 =====
-- -- KEYS: 库存、已购买用户集合、用户对应的预扣订单号
-- -- ARGV: 用户ID、订单ID
-- local stock = tonumber(redis.call('get', KEYS[1]) or '0')
-- if stock <= 0 then
--     return 1
-- end
-- if redis.call('sismember', KEYS[2], ARGV[1]) == 1 then
--     return 2
-- end
-- redis.call('incrby', KEYS[1], -1)
-- redis.call('sadd', KEYS[2], ARGV[1])
-- redis.call('hset', KEYS[3], ARGV[1], ARGV[2])
-- -- 消息改由应用发布到RabbitMQ，不再写入Redis Stream。
-- return 0
--

-- ===== 当前实现：Hash校验一人一单，并原子登记pending =====
-- KEYS: 库存Key、优惠券预占Hash、全局待对账ZSet
-- ARGV: userId、orderId、voucherId、预占时间戳（毫秒）
if redis.call('hexists', KEYS[2], ARGV[1]) == 1 then
    return 2
end
local stock = tonumber(redis.call('get', KEYS[1]) or '0')
if stock <= 0 then
    return 1
end
redis.call('decr', KEYS[1])
redis.call('hset', KEYS[2], ARGV[1], ARGV[2])
redis.call('zadd', KEYS[3], ARGV[4], ARGV[3] .. ':' .. ARGV[1] .. ':' .. ARGV[2])
return 0
