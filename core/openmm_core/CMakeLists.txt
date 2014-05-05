if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Valid Options: Debug, Release, RelWithDebIfno" FORCE)
endif()

find_package(OpenMM REQUIRED)
include_directories(${OPENMM_INCLUDE_DIRS})

file(GLOB OpenMMCoreSources "*.cpp")
message(STATUS ${OpenMMCoreSources})
list(REMOVE_ITEM OpenMMCoreSources main.cpp)

set(CORE_TYPE "CPU" CACHE STRING "Core type: CPU, OpenCL, or CUDA")

set(CORE_VERSION 2)
add_definitions(-DCORE_VERSION=${CORE_VERSION})

enable_testing()

set(CORE_NAME ocore_${CORE_TYPE})
add_executable(${CORE_NAME} main.cpp)

if(CORE_TYPE STREQUAL "CPU")
    set(ADDITIONAL_LIBRARIES ${FFTW_LIBRARIES})
    add_definitions(-DUSE_PME_PLUGIN)
    add_definitions(-DOPENMM_CPU)
elseif(CORE_TYPE STREQUAL "OpenCL")
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

set(OPENMM_CORE_DEPENDENCIES ${CMAKE_THREAD_LIBS_INIT} ${POCO_LIBRARIES} ${OPENSSL_LIBRARIES} ${OPENMM_LIBRARIES} ${ADDITIONAL_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
mark_as_advanced(OPENMM_CORE_DEPENDENCIES)

add_library(OpenMMCore STATIC ${OpenMMCoreSources})

target_link_libraries(${CORE_NAME} OpenMMCore Core ${OPENMM_CORE_DEPENDENCIES})
add_subdirectory(tests)