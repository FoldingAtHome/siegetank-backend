cmake_minimum_required (VERSION 2.8.5)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_CURRENT_SOURCE_DIR})
set(OPENCL_ROOT /usr/local/cuda CACHE PATH "OpenCL Directory")

find_package(OpenCL REQUIRED)
find_package(POCO REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(FFTW REQUIRED)
find_package(Threads REQUIRED)

include_directories(${POCO_INCLUDE_DIRS})

include_directories(base)
set(CORE_ROOT_DIR ${CMAKE_CURRENT_BINARY_DIR})

enable_testing()

add_subdirectory(base)
#add_subdirectory(openmm_core)
add_subdirectory(tests)