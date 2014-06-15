if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Valid Options: Debug, Release, RelWithDebIfno" FORCE)
endif()

find_package(OpenMM REQUIRED)
include_directories(${OPENMM_INCLUDE_DIRS})

file(GLOB OpenMMCoreSources "*.cpp")
message(STATUS ${OpenMMCoreSources})
list(REMOVE_ITEM OpenMMCoreSources main.cpp)

set(CORE_TYPE "CPU" CACHE STRING "Core type: CPU, OpenCL, or CUDA")

set(BUILD_FAH_CORE OFF CACHE BOOL "Whether or not to build as a FAHCore")

set(CORE_VERSION 15 CACHE STRING "Core Version")
add_definitions(-DCORE_VERSION=${CORE_VERSION})

enable_testing()

if(BUILD_FAH_CORE)
    add_definitions(-DFAH_CORE)
    set(CORE_NAME ocore_601_${CORE_TYPE}_v${CORE_VERSION}_fah)
else()
    set(CORE_NAME ocore_601_${CORE_TYPE}_v${CORE_VERSION})
endif()

add_executable(${CORE_NAME} main.cpp)

set(CPU_ENGINE_KEY 86f094c5-b76a-460e-9335-61eb0c76643b)
set(OPENCL_ENGINE_KEY 3ca62f73-3979-4eb2-a302-d891ad75b70b)

if(CORE_TYPE STREQUAL "CPU")
    set(ADDITIONAL_LIBRARIES ${FFTW_LIBRARIES})
    add_definitions(-DENGINE_KEY="${CPU_ENGINE_KEY}")
    add_definitions(-DUSE_PME_PLUGIN)
    add_definitions(-DOPENMM_CPU)
elseif(CORE_TYPE STREQUAL "OpenCL")
    add_definitions(-DENGINE_KEY="${OPENCL_ENGINE_KEY}")
    find_package(OpenCL REQUIRED)
    message(STATUS "DEBUG!" ${OPENCL_INCLUDE_DIRS})
    include_directories(${OPENCL_INCLUDE_DIRS})
    set(ADDITIONAL_LIBRARIES ${OPENCL_LIBRARIES})
    add_definitions(-DOPENMM_OPENCL)
elseif(CORE_TYPE STREQUAL "CUDA")
    set(CUDA_DRIVER_PATH "/usr/lib/nvidia-current/" CACHE STRING "Driver Location")
    set(CUFFT_PATH "/usr/local/cuda/lib64" CACHE STRING "CUFFT Location")
    link_directories(${CUDA_DRIVER_PATH})
    link_directories(${CUFFT_PATH})
    set(ADDITIONAL_LIBRARIES cuda cufft)
    add_definitions(-DOPENMM_CUDA)
else()
    message(FATAL_ERROR "Bad CORE_TYPE" )
endif()

set(OPENMM_CORE_DEPENDENCIES ${CMAKE_THREAD_LIBS_INIT} ${POCO_LIBRARIES} ${OPENSSL_LIBRARIES} ${OPENMM_LIBRARIES} ${ADDITIONAL_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT} dl)
mark_as_advanced(OPENMM_CORE_DEPENDENCIES)

add_library(OpenMMCore STATIC ${OpenMMCoreSources})

target_link_libraries(${CORE_NAME} OpenMMCore Core ${OPENMM_CORE_DEPENDENCIES})
add_subdirectory(tests)
