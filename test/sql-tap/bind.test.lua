#!/usr/bin/env tarantool
local test = require("sqltester")
test:plan(1)

-- Make sure that bind variable names cannot start with a number.
test:do_test(
    "bind-1",
    function()
        local res = {pcall(box.execute, [[SELECT @1asd;]], {{['@1asd'] = 123}})}
        return {tostring(res[3])}
    end, {
        "At line 1 at or near position 9: unrecognized token '1asd'"
    })

test:finish_test()
