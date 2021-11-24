#!/usr/bin/env tarantool

local tap = require('tap')
local test = tap.test('space-upgrade-local-errinj')

test:plan(22)

local functions = {
    -- name, body
    {
        name   = "upgrade_func",
        body   = [[
                  function(tuple)
                      local new_tuple = {}
                      new_tuple[1] = tuple[1]
                      new_tuple[2] = tostring(tuple[2]).."upgraded"
                      return new_tuple
                  end
                 ]],
    }, {
        name   = "upgrade_error",
        body   = [[
                  function(tuple)
                      if tuple[1] >= 15 then error("boom") end
                      local new_tuple = {}
                      new_tuple[1] = tuple[1]
                      new_tuple[2] = tostring(tuple[2]).."upgraded"
                      return new_tuple
                  end
                 ]],
    }
}

-- Indicator to detect tuples affected by upgrade function.
--
local upgraded_suffix = "upgraded"

local function cleanup_functions()
    for _, v in pairs(functions) do
        box.schema.func.drop(v['name'])
    end
end

local function check_space_upgrade_empty()
    local cnt = box.space._space_upgrade.index[0]:count()
    test:is_deeply(cnt, 0, "No entries after running upgrade")
end

local function finalize()
    cleanup_functions()
    box.space.t:drop()
end

local rows_cnt = 20

local function setup()
    box.cfg{}
    -- Create and fill in casual space to test basic capabilities.
    local t = box.schema.create_space("t")
    t:create_index("pk")
    t:format({{"f1", "unsigned"}, {"f2", "unsigned"}})
    for i = 1, rows_cnt do
        t:replace({i, i})
    end

    for _, v in pairs(functions) do
        box.schema.func.create(v['name'], {body = v['body'], is_deterministic = true, is_sandboxed = true})
    end
end

local function reset()
    box.space.t:truncate()
    box.space.t:format({{"f1", "unsigned"}, {"f2", "unsigned"}})
    for i = 1, rows_cnt do
        box.space.t:replace({i, i})
    end
end

local function test_upgrade_on_the_fly()
    test:diag("Test delayed upgrade with accessing obsolete tuples")
    local new_format  = {{name="f1", type="unsigned"},
                         {name="f2", type="string"} }
    box.error.injection.set('ERRINJ_SPACE_UPGRADE_DELAY', true)

    local ok,fib = pcall(box.space.t.upgrade, box.space.t, "notest", "upgrade_func", new_format, true)
    test:is(ok, true, "Upgrade has started")
    -- Since upgrade is executed in a separate fiber, let's give it some
    -- time to process space alter to apply new format.
    --
    require('fiber').sleep(0.1)

    -- Check that tuple is updated via all access methods.
    local get_res = box.space.t.index[0]:get({rows_cnt})
    test:is(get_res[2], tostring(rows_cnt)..upgraded_suffix, "Row is updated via index:get()")
    test:is(fib.status(), "running", "Fiber doing upgrade is running")
    for _, pairs_res in
        box.space.t.index[0]:pairs(rows_cnt, {iterator = box.index.GE}) do
            test:is(pairs_res[2], tostring(rows_cnt)..upgraded_suffix,
                    "Row is updated via index:pairs()")
    end
    local select_res = box.space.t:select({rows_cnt})
    test:is(select_res[1][2], tostring(rows_cnt)..upgraded_suffix,
            "Row is updated via space:select()")

    -- Check also entry in _space_upgrade:
    local upgrade_tuple = box.space._space_upgrade.index[0]:get({box.space.t.id})
    test:is(upgrade_tuple[2], "inprogress", "Entry in _space_upgrade has proper status")

    -- Accessing already upgraded tuples invokes upgrade function anyway.
    -- Due to this fact upgrade function must be idempotent.
    local _ = box.space.t:replace({9, "asd9"})
    _ = box.space.t:replace({10, "asd10"})
    _ = box.space.t:replace({11, "asd11"})
    -- In debug mode upgrade yields each UPGRADE_TX_BATCH_SIZE = 10 tuples,
    -- so let's check the last updated tuple and its neighbours:
    --
    get_res = box.space.t.index[0]:get({9})
    test:is(get_res[2], "asd9"..upgraded_suffix, "Row is updated via index:get()")
    get_res = box.space.t.index[0]:get({10})
    test:is(get_res[2], "asd10"..upgraded_suffix, "Row is updated via index:get()")
    get_res = box.space.t.index[0]:get({11})
    test:is(get_res[2], "asd11"..upgraded_suffix, "Row is updated via index:get()")

    -- Check that we can't run another one upgrade while current one
    -- is in-progress:
    local ok,res = pcall(box.space.t.upgrade, box.space.t, "notest", "upgrade_func", new_format)
    test:is(ok, false, "Upgrade has failed")
    test:is(tostring(res), [[Upgrade of space t failed: upgrade is already in inprogress state]],
            "Proper error message")

    -- Finish upgrade and check rows again.
    fib:set_joinable(true)
    box.error.injection.set('ERRINJ_SPACE_UPGRADE_DELAY', false)
    fib:join()

    get_res = box.space.t.index[0]:get({9})
    test:is(get_res[2], "asd9", "Row is preserved")
    get_res = box.space.t.index[0]:get({10})
    test:is(get_res[2], "asd10", "Row is preserved")
    get_res = box.space.t.index[0]:get({11})
    test:is(get_res[2], "asd11"..upgraded_suffix, "Row is updated by upgrade")

    check_space_upgrade_empty()
end

local function test_upgrade_on_the_fly_error()
    test:diag("Test delayed upgrade with accessing obsolete tuples using raising function")
    local new_format  = {{name="f1", type="unsigned"},
                         {name="f2", type="string"} }
    box.error.injection.set('ERRINJ_SPACE_UPGRADE_DELAY', true)

    local ok, fib = pcall(box.space.t.upgrade, box.space.t, "notest", "upgrade_error", new_format, true)
    test:is(ok, true, "Upgrade has started")

    local ok, get_res = pcall(box.space.t.index[0].get, box.space.t.index[0], rows_cnt)
    test:is(ok, false, "index:get() has failed")
    test:isnumber(string.find(tostring(get_res), "boom"),
                  "Upgrade function failed via index:get()")

    local ok, gen, param, state = pcall(box.space.t.pairs, box.space.t, rows_cnt)
    local ok, pairs_res = pcall(gen, param, state)
    test:isnumber(string.find(tostring(pairs_res), "boom"),
                "Upgrade function failed via index:pairs()")

    local ok, select_res = pcall(box.space.t.select, box.space.t, rows_cnt)
    test:isnumber(string.find(tostring(select_res), "boom"),
                  "Upgrade function failed via space:select()")

    fib:set_joinable(true)
    box.error.injection.set('ERRINJ_SPACE_UPGRADE_DELAY', false)
    fib:join()

    local ok, res = pcall(box.space.t.upgrade, box.space.t, "notest", "upgrade_func", new_format)
    test:is(ok, true, "Upgrade has finished")

    check_space_upgrade_empty()
end

local function run_all()
    setup()
    test_upgrade_on_the_fly()
    reset()
    test_upgrade_on_the_fly_error()
    finalize()
end

run_all()

os.exit(test:check() and 0 or 1)
