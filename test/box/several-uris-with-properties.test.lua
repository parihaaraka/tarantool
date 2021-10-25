test_run = require('test_run').new()
net_box = require('net.box')
fio = require('fio')

default_listen_addr = box.cfg.listen
unix_socket_path = "unix/:" .. "./tarantool"
-- Check ability to pass several listening uris in one string
-- with different properties
test_run:cmd("setopt delimiter ';'")
valid_listen_uris = {
    unix_socket_path .. "A" .. "," .. unix_socket_path .. "B",
    unix_socket_path .. "A" .. "?backlog=10;20",
    unix_socket_path .. "A" .. "?backlog=10&backlog=20",
    unix_socket_path .. "A" .. "?backlog=10;20&backlog=30;40",
    unix_socket_path .. "A" .. "?backlog=10&readahead=2048",
    unix_socket_path .. "A" .. "?backlog=10;20&readahead=2048;4096",
    unix_socket_path .. "A" .. "?backlog=10&backlog=20" .. "&" ..
                               "readahead=2048&readahead=4096",
    unix_socket_path .. "A" .. "?backlog=10;20&backlog=30;40" .. "&" ..
                               "readahead=2048;4096&readahead=8192;16384",
    unix_socket_path .. "A" .. "?backlog=10;20&backlog=30;40" .. "&" ..
                               "readahead=2048;4096&readahead=8192;16384" ..
                               ", " ..
    unix_socket_path .. "B" .. "?backlog=10;20&backlog=30;40" .. "&" ..
                               "readahead=2048;4096&readahead=8192;16384" ..
                               ", " ..
    unix_socket_path .. "C" .. "?backlog=10;20&backlog=30;40" .. "&" ..
                               "readahead=2048;4096&readahead=8192;16384"
};
test_run:cmd("setopt delimiter ''");