local ffi = require('ffi')

ffi.cdef([[
    int txm_bench_init(unsigned);
    double txm_bench_run(unsigned);
]])
local tx_man_bench = ffi.load(package.searchpath('perf/tx_man', './?.so'))


local run = function(space_id)
    box.cfg{
        memtx_use_mvcc_engine = true,
    }
    space_id = space_id + box.schema.SYSTEM_ID_MAX

    local space_name = 'space_1'
    local space = box.schema.create_space(space_name,{ engine = 'memtx', id = space_id })
    space:create_index('pk', { type = 'TREE', parts = {1, 'unsigned'} })
    space:create_index('sk', { type = 'HASH', parts = {2, 'string'} })

    tx_man_bench.txm_bench_init(0xdeadbeef)
    tx_man_bench.txm_bench_run(space_id)
end

run(1)