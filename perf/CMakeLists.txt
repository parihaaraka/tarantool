set(CMAKE_CXX_STANDARD 11)

find_package(benchmark QUIET)
if (NOT ${benchmark_FOUND})
    message(AUTHOR_WARNING "Google Benchmark library was not found")
    return()
endif()

include_directories(${MSGPUCK_INCLUDE_DIRS})
include_directories(${PROJECT_SOURCE_DIR}/src/box)
include_directories(${CMAKE_SOURCE_DIR}/third_party)

add_executable(tuple.perftest tuple.cc)
target_link_libraries(tuple.perftest core box tuple benchmark::benchmark)

add_library(tx_man SHARED tx_man.cc)
target_link_libraries(tx_man ${MSGPUCK_LIBRARIES})
set_target_properties(tx_man PROPERTIES PREFIX "" OUTPUT_NAME "tx_man")