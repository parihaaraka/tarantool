# Tarantool

[![Actions Status][actions-badge]][actions-url]
[![Code Coverage][coverage-badge]][coverage-url]
[![Telegram][telegram-badge]][telegram-url]
[![GitHub Discussions][discussions-badge]][discussions-url]
[![Stack Overflow][stackoverflow-badge]][stackoverflow-url]

[Tarantool][tarantool-url] is an in-memory database and application server.

It is distributed under [BSD 2-Clause][license] terms.

Key features of the application server:

* 100% compatible drop-in replacement for Lua 5.1, based on LuaJIT 2.1.
  Simply use `#!/usr/bin/tarantool` instead of `#!/usr/bin/lua` in your script.
* Full support for Lua modules and a rich set of [own modules][modules],
  including cooperative multitasking, non-blocking I/O, access to external
  databases, [persistent queues][queue], [sharding][vshard], [cluster and
  application management framework][cartridge], etc.

Key features of the database:

* MessagePack data format and MessagePack based client-server protocol.
* Two data engines: 100% in-memory with optional persistence and an own
  implementation of LSM-tree, to use with large data sets.
* Multiple index types: HASH, TREE, RTREE, BITSET.
* Document oriented JSON path indexes.
* Asynchronous master-master replication.
* Synchronous quorum-based replication.
* RAFT-based automatic leader election.
* Authentication and access control.
* ANSI SQL, including views, joins, referential and check constraints.
* [Connectors][connectors] for many programming languages.
* The database is just a C extension to the application server and can be
  turned off.

Supported platforms are Linux (x86_64, aarch64), Mac OS X (x86_64, M1), FreeBSD
(x86_64).

Tarantool is ideal for data-enriched components of scalable Web architecture:
queue servers, caches, stateful Web applications.

To download and install Tarantool as a binary package for your OS or using
docker, please visit [download instructions][download].

To build Tarantool from source, see detailed [instructions][building] in the
Tarantool documentation.

To find modules, connectors and tools for Tarantool, check out our [Awesome
Tarantool][awesome-list] list.

Please report bugs at [issue tracker][issue-tracker]. We also warmly welcome
your feedback on the [discussions][discussions-url] page and questions on
[Stack Overflow][stackoverflow-url].

We accept contributions via pull requests. Check out our [How to get
involved][get-involved] guide.

Thank you for your interest in Tarantool!

[actions-badge]: https://github.com/tarantool/tarantool/workflows/release/badge.svg
[actions-url]: https://github.com/tarantool/tarantool/actions
[coverage-badge]: https://coveralls.io/repos/github/tarantool/tarantool/badge.svg?branch=master
[coverage-url]: https://coveralls.io/github/tarantool/tarantool?branch=master
[telegram-badge]: https://img.shields.io/badge/Telegram-join%20chat-blue.svg
[telegram-url]: http://telegram.me/tarantool
[discussions-badge]: https://img.shields.io/github/discussions/tarantool/tarantool
[discussions-url]: https://github.com/tarantool/tarantool/discussions
[stackoverflow-badge]: https://img.shields.io/badge/stackoverflow-tarantool-orange.svg
[stackoverflow-url]: https://stackoverflow.com/questions/tagged/tarantool
[tarantool-url]: https://www.tarantool.io/en/
[license]: LICENSE
[modules]: https://www.tarantool.io/en/download/rocks
[queue]: https://github.com/tarantool/queue
[vshard]: https://github.com/tarantool/vshard
[cartridge]: https://github.com/tarantool/cartridge
[connectors]: https://www.tarantool.io/en/download/connectors
[download]: https://www.tarantool.io/en/download/
[building]: https://www.tarantool.io/en/doc/latest/dev_guide/building_from_source/
[issue-tracker]: https://github.com/tarantool/tarantool/issues
[get-involved]: https://www.tarantool.io/en/doc/latest/contributing/contributing/
[awesome-list]: https://github.com/tarantool/awesome-tarantool/
