cmake_minimum_required (VERSION 2.6)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Valid Options: Debug, Release, RelWithDebIfno" FORCE)
endif()

file(GLOB CoreSources "*.cpp")
set(POCO_INSTALL_DIR "~/poco152_install" CACHE FILEPATH "Poco 1.5.2 install folder")
find_package(OpenSSL)
include_directories("${POCO_INSTALL_DIR}/include")

set(CORE_VERSION 1)
add_definitions(-DCORE_VERSION=${CORE_VERSION})
link_directories(${POCO_INSTALL_DIR}/lib)
set(poco_libraries PocoNetSSL PocoCrypto PocoUtil PocoJSON PocoXML PocoNet PocoFoundation)

add_library(Core ${CoreSources})
