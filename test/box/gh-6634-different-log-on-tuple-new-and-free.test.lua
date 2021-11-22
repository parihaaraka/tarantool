env = require('test_run')
test_run = env.new()

--
s = box.schema.create_space('test')
_ = s:create_index('pk')

default_log_lvl = box.cfg.log_level
box.cfg{log_level = 7}

_ = s:replace{0, 0, 0, 0, 0, 0, 0}
str = test_run:grep_log('default', 'tuple_new%(%d+%) = 0x%x+$')
new_tuple_address = string.match(str, '0x%x+$')
new_tuple_address = string.sub(new_tuple_address, 3)

collectgarbage('collect')

-- Replace previously inserted tuple with new one
-- to activate memtx_gc and delete the tuple.
_ = s:replace{0, 1}

collectgarbage('collect')
box.cfg{log_level = default_log_lvl}

-- Check if last deleted tuple is the tuple inserted above.
str = test_run:grep_log('default', 'tuple_delete%(0x%x+%)')
deleted_tuple_address = string.match(str, '0x%x+%)')
deleted_tuple_address = string.sub(deleted_tuple_address, 3, -2)
assert(new_tuple_address == deleted_tuple_address)

s:drop()
