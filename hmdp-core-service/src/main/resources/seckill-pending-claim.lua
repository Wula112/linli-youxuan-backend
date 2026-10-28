-- 只推进仍存在且已超时的成员，不允许ZADD重新加入已被消费端清理的记录。
-- ARGV: 完整member、超时截止时间、当前时间
local score = redis.call('zscore', KEYS[1], ARGV[1])
if not score or tonumber(score) > tonumber(ARGV[2]) then
    return 0
end
redis.call('zadd', KEYS[1], 'XX', ARGV[3], ARGV[1])
return 1
