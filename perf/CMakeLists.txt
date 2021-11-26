set(CMAKE_CXX_STANDARD 11)

find_package(benchmark QUIET)
if (NOT ${benchmark_FOUND})
    message(AUTHOR_WARNING "Google Benchmark library was not found")
    return()
endif()

find_package(RapidJSON QUIET)
if (NOT ${RapidJSON_FOUND})
    message(AUTHOR_WARNING "Rapid JSON library was not found,
            not all benchmars are available")
else ()
    add_definitions(-DRAPID_JSON_FOUND)
endif()

include_directories(${MSGPUCK_INCLUDE_DIRS})
include_directories(${PROJECT_SOURCE_DIR}/src/box)
include_directories(${CMAKE_SOURCE_DIR}/third_party)

add_executable(tuple.perftest tuple.cc)
target_link_libraries(tuple.perftest core box tuple benchmark::benchmark)

add_executable(compression.perftest compression.cc)
target_link_libraries(compression.perftest core box tuple benchmark::benchmark)
