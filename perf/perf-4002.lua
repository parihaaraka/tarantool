fiber = require('fiber')
os = require('os')

local N = 1000000
local looped = 1
local cnt = 1000000

function empty() end

function main()
    for i = 1, cnt do fiber.create(empty) end
    return cnt
end

function recursive_caller(n)
    if n ~= 0 then return recursive_caller(n-1)
    else return main()
    end
end

function tail_caller_foo(n)
    return tail_caller_bar(n)
end

function tail_caller_bar(n)
    if n ~= 0 then return tail_caller_foo(n-1)
    else return main()
    end
end

fiber.parent_bt_disable()

local nobt_rec_time = os.clock()
for _ = 1, looped do
    fiber.create(recursive_caller, cnt)
end
nobt_rec_time = os.clock() - nobt_rec_time

local nobt_tail_time = os.clock()
for _ = 1, looped do
    fiber.create(tail_caller_foo, cnt)
end
nobt_tail_time = os.clock() - nobt_tail_time

fiber.parent_bt_enable()

local bt_rec_time = os.clock()
for _ = 1, looped do
    fiber.create(recursive_caller, cnt)
end
bt_rec_time = os.clock() - bt_rec_time

local bt_tail_time = os.clock()
for _ = 1, looped do
    fiber.create(tail_caller_foo, cnt)
end
bt_tail_time = os.clock() - bt_tail_time

print('WITH BT REC CALL: ', bt_rec_time, 'RATE: fib/sec', N / bt_rec_time)
print('NO BT REC CALL: ', nobt_rec_time, 'RATE: fib/sec', N / nobt_rec_time)
print('WITH BT TAIL CALL: ', bt_tail_time, 'RATE: fib/sec', N / bt_tail_time)
print('NO BT TAIL CALL: ', nobt_tail_time, 'RATE: fib/sec', N / nobt_tail_time)
