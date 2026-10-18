-- 1. 优惠券ID
local voucherId = ARGV[1]
-- 2. 用户ID
local userId = ARGV[2]
-- 3. 订单ID
local orderId = ARGV[3]

--库存Key
local stockKey = 'seckill:stock:' .. voucherId
--优惠券订单Key
local orderKey = 'seckill:order:' .. voucherId

-- 3. 查询库存是否充足
if(tonumber(redis.call('get', stockKey)) <= 0) then
    -- 3.1 库存不足，返回1
    return 1
end

-- 3.2 校验一人一单
if(redis.call('sismember', orderKey, userId) == 1) then
    -- 3.3 购买过，返回2
    return 2
end
-- 预扣库存
redis.call('incrby', stockKey, -1)
redis.call('sadd', orderKey, userId)
-- 将订单信息加入消息队列 XADD key ID|* field value [field value ...]
redis.call('xadd','stream.orders','*','userId', userId, 'voucherId', voucherId, 'id', orderId)
return 0

