local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId = ARGV[3]
local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId

-- 判断库存是否充足
if (tonumber(redis.call('get', stockKey)) <= 0) then
    return 1
end

-- 判断用户是否已经购买过
if (redis.call('sismember', orderKey, userId) == 1) then
    return 2
end

-- 扣减库存，并记录用户购买信息
redis.call('incrby', stockKey, -1)
redis.call('sadd', orderKey, userId)
redis.call('XADD','streams.order','*','userId',userId,'voucherId',voucherId,'id',orderId)
return 0