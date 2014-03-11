cmake_minimum_required (VERSION 2.8)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_CURRENT_SOURCE_DIR})

find_package(FFTW REQUIRED)

set(BUILD_BENCHMARK TRUE CACHE BOOL "Whether or not to build the benchmark")

if(BUILD_BENCHMARK)
    add_subdirectory(benchmark)
endif()

set(CORE_ROOT_DIR ${CMAKE_CURRENT_BINARY_DIR})

add_subdirectory(tests)