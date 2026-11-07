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

-- KEYS: 库存、预占Hash、pending、旧补发计数Hash、本订单释放凭证、未建单请求Hash。
-- ARGV: userId、orderId、完整pending member。凭证保存完整标识，不设置TTL。
local released = redis.call('get', KEYS[5])
local owner = redis.call('hget', KEYS[2], ARGV[1])
local function clearRequest()
    if redis.call('hget', KEYS[6], ARGV[2]) == ARGV[3] then
        redis.call('hdel', KEYS[6], ARGV[2])
    end
end
if released then
    if released ~= ARGV[3] then
        return -2
    end
    -- 上次库存和凭证已写入，即使清理中途失败也只能继续清理，不能再次加库存。
    if owner == ARGV[2] then
        redis.call('hdel', KEYS[2], ARGV[1])
    end
    redis.call('zrem', KEYS[3], ARGV[3])
    redis.call('hdel', KEYS[4], ARGV[3])
    clearRequest()
    return 2
end
if owner ~= ARGV[2] then
    -- 缺失和归属变化都不能证明本订单曾返库。
    return 0
end
local stock = tonumber(redis.call('get', KEYS[1]))
if not stock or stock < 0 or stock % 1 ~= 0 or stock >= 9007199254740991 then
    return -1
end
-- 同一个写命令同时更新库存和凭证；之后清理出错时重试不会重复加库存。
redis.call('mset', KEYS[1], string.format('%.0f', stock + 1), KEYS[5], ARGV[3])
redis.call('hdel', KEYS[2], ARGV[1])
redis.call('zrem', KEYS[3], ARGV[3])
redis.call('hdel', KEYS[4], ARGV[3])
clearRequest()
return 1
