#!/usr/bin/env tarantool

local test = require('tester')
local date = require('datetime')

local engine = test:engine()

test:plan(1)
test:test("Simple engines tests for datetime indices", function(test)
    test:plan(23)
    local tmp = box.schema.space.create('tmp', {engine = engine})
    tmp:create_index('pk', {parts={1,'datetime'}})

    tmp:insert{date.parse('1970-01-01')}
    tmp:insert{date.parse('1970-01-02')}
    tmp:insert{date.parse('1970-01-03')}
    tmp:insert{date.parse('2000-01-01')}

    local rs = tmp:select{}
    test:is(tostring(rs[1][1]), '1970-01-01T00:00:00Z')
    test:is(tostring(rs[2][1]), '1970-01-02T00:00:00Z')
    test:is(tostring(rs[3][1]), '1970-01-03T00:00:00Z')
    test:is(tostring(rs[4][1]), '2000-01-01T00:00:00Z')

    for _ = 1,16 do
        tmp:insert{date.now()}
    end

    local a = tmp:select{}
    for i = 1, #a - 1 do
        test:ok(a[i][1] < a[i + 1][1], ('%s < %s'):format(a[i][1], a[i + 1][1]))
    end

    tmp:drop()
end)

os.exit(test:check() and 0 or 1)
