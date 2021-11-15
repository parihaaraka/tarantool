#!/usr/bin/env tarantool

local fiber = require('fiber')
local tap = require('tap')
local popen = require('popen')

--
-- gh-2717: tarantool console quit on sigint
--

local TARANTOOL_PATH = arg[-1]

local res = tap.test('gh-2717', function(test)
    print(TARANTOOL_PATH)

    test:plan(3)
    local prompt = "tarantool>"
    local ph = popen.shell(TARANTOOL_PATH .. " -i", 'r')
    assert(ph.pid, "popen error while executing" .. TARANTOOL_PATH)

    local res = "tarantool> \n"
    fiber.sleep(0.5)
    ph:signal(popen.signal.SIGINT)
    fiber.sleep(0.5)
    res = res .. "tarantool> \n"

    local output = ph:read()
    test:like(ph:info().status.state, popen.state.ALIVE, " process stays being alive after SIGINT ")

    ph:close()
    print(output)
    test:like(output, prompt .. " \n" .. prompt, " SIGINT doesn't kill tarantool on interactive mode ")

    -- Check if daemon process exits after SIGINT

    local phd = popen.shell(TARANTOOL_PATH .. " -e 'box.cfg{listen=3310, pid_file=\'file\'}'", 'r')
    assert(phd.pid, "popen error while executing" .. TARANTOOL_PATH)

    fiber.sleep(0.5)
    phd:signal(popen.signal.SIGINT)
    fiber.sleep(0.5)

    test:like(phd:info().status.state, popen.state.EXITED, " daemon process exited after SIGINT ")
end)
os.exit(res and 0 or 1)