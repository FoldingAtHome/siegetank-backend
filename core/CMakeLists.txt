cmake_minimum_required (VERSION 2.6)

file(GLOB CoreSources "src/*.cpp")
file(GLOB OpenMMCoreSources "src/openmm/*.cpp")
file(GLOB TeraChemCoreSource "src/terachem/*.cpp")

set(POCO_INSTALL_DIR "~/poco152_install" CACHE FILEPATH "Poco 1.5.2 install folder")
set(OPENMM_INSTALL_DIR "~/openmm_install" CACHE FILEPATH "OpenMM 1.5.2 install folder")

find_package(OpenSSL)
find_package(Threads)

include_directories("${PROJECT_SOURCE_DIR}/include")
include_directories("${PROJECT_SOURCE_DIR}/include/openmm")
include_directories("${POCO_INSTALL_DIR}/include")
include_directories("${OPENMM_INSTALL_DIR}/include")

set(CORE_VERSION 1)
add_definitions(-DCORE_VERSION=${CORE_VERSION})

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Valid Options: Debug, Release, RelWithDebIfno" FORCE)
endif()

enable_testing()

add_subdirectory(tests)