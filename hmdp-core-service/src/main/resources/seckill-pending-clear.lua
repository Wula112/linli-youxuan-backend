-- KEYS: pending ZSet、旧版补发次数Hash；ARGV: 完整member
-- 旧计数只做兼容清理，不再新增计数；不修改购买资格。
local removed = redis.call('zrem', KEYS[1], ARGV[1])
redis.call('hdel', KEYS[2], ARGV[1])
return removed
