cmake_minimum_required (VERSION 2.6)

set(BUILD_BENCHMARK TRUE CACHE BOOL "Whether or not to build the benchmark")

if(BUILD_BENCHMARK)
    add_subdirectory(benchmark)
endif()

set(CORE_ROOT_DIR ${CMAKE_CURRENT_BINARY_DIR})

add_subdirectory(tests)