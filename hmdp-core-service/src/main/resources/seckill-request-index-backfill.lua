-- 旧版本升级：暂停生产者、消费者和调度后分批调用。
-- KEYS: pending ZSet、未建单请求Hash；ARGV: 扫描游标、建议批量大小。
-- 返回下次游标；返回 0 表示本轮完成。HSETNX 保留已存在的相同索引。
local page = redis.call('zscan', KEYS[1], ARGV[1], 'COUNT', ARGV[2])
local entries = page[2]
local indexed = {}
for i = 1, #entries, 2 do
    local member = entries[i]
    local voucherId, userId, orderId = string.match(member, '^(%d+):(%d+):(%d+)$')
    if not voucherId or not userId or not orderId then
        return redis.error_reply('invalid pending member: ' .. member)
    end
    local previous = redis.call('hget', KEYS[2], orderId)
    if (previous and previous ~= member) or (indexed[orderId] and indexed[orderId] ~= member) then
        return redis.error_reply('conflicting order request: ' .. orderId)
    end
    indexed[orderId] = member
end
for orderId, member in pairs(indexed) do
    redis.call('hsetnx', KEYS[2], orderId, member)
end
return page[1]
