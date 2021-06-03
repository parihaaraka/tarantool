yaml = require('yaml')
fiber = require('fiber')
test_run = require('test_run').new()

stack_len = 0
parent_stack_len = 0

test_run:cmd('setopt delimiter ";"')
function foo()
    local fiber_info = fiber.info()
    local fiber_id = fiber.self():id()
    local parent_stack = fiber_info[fiber_id].backtrace_parent
    stack_len = stack and #stack or 0
    parent_stack_len = parent_stack and #parent_stack or 0
end;

function bar()
    fiber.create(foo)
end;
test_run:cmd('setopt delimiter ""');

bar()
 -- ... -> fiber.create() -> fiber.new() -> fiber.new_ex() 
 -- or backtrace is disabled
parent_stack_len > 3 or stack_len == 0
