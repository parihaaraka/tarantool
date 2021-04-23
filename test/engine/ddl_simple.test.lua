test_run = require('test_run')
inspector = test_run.new()
engine = inspector:get_cfg('engine')

--
-- Check that all modifications done to the space during index build
-- are reflected in the new index.
--
math.randomseed(os.time())

s = box.schema.space.create('test', {engine = engine})
_ = s:create_index('pk')
inspector:cmd("setopt delimiter ';'")

last_val = 10;
box.begin()
for i = 1, last_val do box.space.test:replace{i, i, i} end
box.commit();

function gen_load()
    local s = box.space.test
    for i = 1, 10 do
        local key = math.random(last_val)
        local val1 = math.random(last_val)
        local val2 = last_val + 1
        last_val = val2
        pcall(s.upsert, s, {key, val1, val2}, {{'=', 2, val1}, {'=', 3, val2}})
    end
end;

function check_fiber()
    _ = fiber.create(function() gen_load() ch:put(true) end)
    _ = box.space.test:create_index('sk', {unique = false, parts = {2, 'unsigned'}})

    assert(ch:get(10) == true)

    local index = box.space.test.index
    if index.pk:count() ~= index.sk:count() then
        require('log').error("Error on fiber check: failed '1st step secondary keys' check on equal" ..
                             " pk = " .. index.pk:count() .. " and k = " .. index.sk:count())
        return false
    end

    return true
end;

inspector:cmd("setopt delimiter ''");

fiber = require('fiber')
ch = fiber.channel(1)
check_fiber()

box.space.test:drop()
