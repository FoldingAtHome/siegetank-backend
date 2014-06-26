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

set(CORE_VERSION 17 CACHE STRING "Core Version")
add_definitions(-DCORE_VERSION=${CORE_VERSION})

if(WIN32)
    add_definitions(-DPTW32_STATIC_LIB)
    add_definitions(-DLEPTON_USE_STATIC_LIBRARIES)
    add_definitions(-DOPENMM_USE_STATIC_LIBRARIES)
endif()

enable_testing()

if(BUILD_FAH_CORE)
    add_definitions(-DFAH_CORE)
    set(CORE_NAME core19_601)
else()
    set(CORE_NAME ocore_601)
endif()

set(CPU_ENGINE_KEY 86f094c5-b76a-460e-9335-61eb0c76643b)
set(OPENCL_ENGINE_KEY 3ca62f73-3979-4eb2-a302-d891ad75b70b)
set(CUDA_ENGINE_KEY 91c5e3f7-f6fa-446b-843c-e94bf5ed49e5)

if(UNIX)
    set(BUILD_CPU ON CACHE BOOL "Whether to build CPU core or not")
elseif(WIN32)
    set(BUILD_CPU OFF CACHE BOOL "Whether to build CPU core or not")
endif()
set(BUILD_OPENCL ON CACHE BOOL "Whether to build OpenCL core or not")
set(BUILD_CUDA ON CACHE BOOL "Whether to build CUDA core or not")

message(STATUS ${POCO_LIBRARIES})

set(OPENMM_CORE_DEPENDENCIES ${CMAKE_THREAD_LIBS_INIT} ${POCO_LIBRARIES} ${OPENSSL_LIBRARIES} ${OPENMM_LIBRARIES})

if(UNIX)
    set(OPENMM_CORE_DEPENDENCIES ${OPENMM_CORE_DEPENDENCIES} dl)
else()
    set(OPENMM_CORE_DEPENDENCIES ${OPENMM_CORE_DEPENDENCIES} Crypt32)
endif()

if(BUILD_CPU)
    find_package(FFTW REQUIRED)
    set(CPU_CORE_NAME ${CORE_NAME}_CPU_v${CORE_VERSION})
    add_executable(${CPU_CORE_NAME} main.cpp ${OpenMMCoreSources})
    set_target_properties(${CPU_CORE_NAME} PROPERTIES COMPILE_DEFINITIONS "ENGINE_KEY=\"${CPU_ENGINE_KEY}\";USE_PME_PLUGIN;OPENMM_CPU")
    target_link_libraries(${CPU_CORE_NAME} Core ${OPENMM_CORE_DEPENDENCIES} ${FFTW_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
endif()

if(BUILD_OPENCL)
    set(OPENCL_CORE_NAME ${CORE_NAME}_OpenCL_v${CORE_VERSION})
    add_executable(${OPENCL_CORE_NAME} main.cpp  ${OpenMMCoreSources})
    set_target_properties(${OPENCL_CORE_NAME} PROPERTIES COMPILE_DEFINITIONS "ENGINE_KEY=\"${OPENCL_ENGINE_KEY}\";OPENMM_OPENCL")
    find_package(OpenCL REQUIRED)
    include_directories(${OPENCL_INCLUDE_DIRS})
    target_link_libraries(${OPENCL_CORE_NAME} Core ${OPENMM_CORE_DEPENDENCIES} ${OPENCL_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
endif()

if(BUILD_CUDA)
    set(CUDA_CORE_NAME ${CORE_NAME}_CUDA_v${CORE_VERSION})
    add_executable(${CUDA_CORE_NAME} main.cpp ${OpenMMCoreSources})
    set_target_properties(${CUDA_CORE_NAME} PROPERTIES COMPILE_DEFINITIONS "ENGINE_KEY=\"${CUDA_ENGINE_KEY}\";OPENMM_CUDA")
    set(CUDA_DRIVER_PATH "/usr/lib/libcuda.so" CACHE STRING "CUDA Driver Location")
    set(CUFFT_PATH "/usr/local/cuda/lib64/libcufft.so" CACHE STRING "CUFFT Location")
    target_link_libraries(${CUDA_CORE_NAME} Core ${OPENMM_CORE_DEPENDENCIES} ${CUFFT_PATH} ${CUDA_DRIVER_PATH} ${CMAKE_THREAD_LIBS_INIT})
endif()

# add_subdirectory(tests)
