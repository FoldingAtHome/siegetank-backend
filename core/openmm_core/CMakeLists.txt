if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Valid Options: Debug, Release, RelWithDebIfno" FORCE)
endif()

file(GLOB OpenMMCoreSources "*.cpp")
set(OpenMMCoreSources ${OpenMMCoreSources})

set(OPENMM_INSTALL_DIR "~/openmm_install" CACHE FILEPATH "OpenMM 6.0 install folder")
set(CORE_TYPE "CPU" CACHE STRING "Core type: CPU, OpenCL, or CUDA")

find_package(OpenSSL)
find_package(Threads)

include_directories("${OPENMM_INSTALL_DIR}/include")
set(CORE_VERSION 1)
add_definitions(-DCORE_VERSION=${CORE_VERSION})

link_directories(${OPENMM_INSTALL_DIR}/lib)
link_directories(${OPENMM_INSTALL_DIR}/lib/plugins)
set(openmm_libraries OpenMM_static)

enable_testing()

set(ADDITIONAL_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})

if(CORE_TYPE STREQUAL "CPU")
    set(ADDITIONAL_LIBRARIES OpenMMCPU_static OpenMMPME_static ${FFTW_LIBRARIES})
    add_definitions(-DUSE_PME_PLUGIN)
    add_definitions(-DOPENMM_CPU)
elseif(CORE_TYPE STREQUAL "OpenCL")
    set(ADDITIONAL_LIBRARIES OpenMMOpenCL_static ${OPENCL_LIBRARIES})
    add_definitions(-DOPENMM_OPENCL)
elseif(CORE_TYPE STREQUAL "CUDA")
    set(DRIVER_PATH "/usr/lib/nvidia-current/" CACHE STRING "Driver Location")
    set(CUFFT_PATH "/usr/local/cuda/lib64" CACHE STRING "CUFFT Location")
    link_directories(${DRIVER_PATH})
    link_directories(${CUFFT_PATH})
    set(openmm_libraries OpenMMCUDA_static cuda cufft ${openmm_libraries})
    add_definitions(-DOPENMM_CUDA)
else()
    message(FATAL_ERROR "Bad CORE_TYPE" )
endif()

set(CORE_NAME ocore_${CORE_TYPE})

add_executable(${CORE_NAME} ${OpenMMCoreSources})
# Note: order matters here!
target_link_libraries(${CORE_NAME} Core ${POCO_LIBRARIES} ${OPENSSL_LIBRARIES} ${openmm_libraries} ${ADDITIONAL_LIBRARIES})