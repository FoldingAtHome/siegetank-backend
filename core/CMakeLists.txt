cmake_minimum_required (VERSION 2.8.5)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_CURRENT_SOURCE_DIR})
set(OPENCL_ROOT /usr/local/cuda CACHE PATH "OpenCL Directory")

find_package(POCO REQUIRED)

if(WIN32)
	set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")
endif()

if(WIN32)
	set(OPENSSL_ROOT_DIR "C:/Libs/openssl")
else()
	set(OPENSSL_ROOT_DIR "/home/yutong/openssl_install")
endif()

find_package(OpenSSL2 REQUIRED)

message(STATUS "OPENSSL:" ${OPENSSL_LIBRARIES})

find_package(Threads REQUIRED)

include_directories(${POCO_INCLUDE_DIRS})
include_directories(${OPENSSL_INCLUDE_DIR})
include_directories(base)
set(CORE_ROOT_DIR ${CMAKE_CURRENT_BINARY_DIR})

enable_testing()

add_subdirectory(base)
add_subdirectory(openmm_core)
add_subdirectory(tests)