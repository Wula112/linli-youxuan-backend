-- KEYS: pending ZSet、补发次数Hash；ARGV: 完整member
-- 与补发计数原子互斥，避免订单完成后又留下孤立计数，不修改购买资格。
local removed = redis.call('zrem', KEYS[1], ARGV[1])
redis.call('hdel', KEYS[2], ARGV[1])
return removed
