-- KEYS[1]: 限流ZSet；ARGV: 窗口毫秒数、最大放行数、请求唯一ID
local key = KEYS[1]
local window = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
if not window or window <= 0 or window ~= math.floor(window)
        or not limit or limit <= 0 or limit ~= math.floor(limit)
        or not ARGV[3] or ARGV[3] == '' then
    return redis.error_reply('Invalid rate limit parameters')
end

-- 使用Redis统一时钟，避免多个应用实例的本地时间偏差影响同一个窗口。
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
-- 窗口为(now-window, now]；到达边界的旧请求不再占用额度。
redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
local current = redis.call('ZCARD', key)
if current >= limit then
    return 0
end

redis.call('ZADD', key, now, ARGV[3])
redis.call('PEXPIRE', key, window)
return current + 1
