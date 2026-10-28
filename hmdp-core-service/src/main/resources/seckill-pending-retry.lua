-- KEYS: pending ZSet、补发次数Hash、当前优惠券的预占Hash
-- ARGV: 完整member、最大补发轮次、userId、orderId
-- 返回：正数为本次补发轮次，-1为达到上限，0为pending或预占已失效。
if not redis.call('zscore', KEYS[1], ARGV[1]) then
    return 0
end
if redis.call('hget', KEYS[3], ARGV[3]) ~= ARGV[4] then
    return 0
end
local retries = tonumber(redis.call('hget', KEYS[2], ARGV[1]) or '0')
if retries >= tonumber(ARGV[2]) then
    return -1
end
-- 先占用次数再发送：发送失败或进程退出也不返还，保证累计次数不突破上限。
return redis.call('hincrby', KEYS[2], ARGV[1], 1)
