include(ExternalProject)

if(APPLE)
    find_library(OPENCL_LIBRARY OpenCL)
elseif(UNIX)
    set(OPENCL_ROOT /usr/local/cuda CACHE PATH "OpenCL Directory")
endif()

ExternalProject_Add(clfft
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/clfft/src
    # do NOT put quotations marks around the PATH
    CMAKE_ARGS -DOPENCL_ROOT:PATH=${OPENCL_ROOT}
        -DCMAKE_INSTALL_PREFIX:PATH=${CMAKE_BINARY_DIR}/clfft_install
    INSTALL_DIR ${CMAKE_BINARY_DIR}/clfft_install
)

file(GLOB BenchmarkSources *.cpp)
ExternalProject_Get_Property(clfft install_dir)
set(CLFFT_INSTALL_DIR ${install_dir})
include_directories(${OPENCL_ROOT}/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/clfft/src/include)
include_directories(${install_dir}/include)
include_directories(${FFTW_INCLUDE_DIRS})
add_library(Benchmark STATIC ${BenchmarkSources})
add_dependencies(Benchmark clfft)