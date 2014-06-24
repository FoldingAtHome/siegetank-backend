cmake_minimum_required (VERSION 2.8)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Valid Options: Debug, Release, RelWithDebIfno" FORCE)
endif()

find_package(Threads REQUIRED)

file(GLOB CoreSources "*.cpp")
find_package(OpenSSL)

set(CORE_VERSION 1)
add_definitions(-DCORE_VERSION=${CORE_VERSION})

add_library(Core ${CoreSources})
