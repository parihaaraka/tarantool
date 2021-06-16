fiber = require('fiber')
os = require('os')

n = 1000000

function empty() end

function main()
    for i = 1, n do
        fiber.create(empty)
    end
end

local nobt_time = os.clock()
fiber.create(main)
nobt_time = os.clock() - nobt_time

fiber.parent_bt_enable()

local bt_time = os.clock()
fiber.create(main)
bt_time = os.clock() - bt_time

print('WITH LUA PARENT BT: ', bt_time, 'RATE: fib/sec', n / bt_time)
print('NO LUA PARENT BT: ', nobt_time, 'RATE: fib/sec', n / nobt_time)
