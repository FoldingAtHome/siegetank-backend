include(ExternalProject)

set(OPENCL_ROOT /usr/local/cuda CACHE PATH "OpenCL Directory")

ExternalProject_Add(clfft
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/clfft/src
    # do NOT put quotations marks around the PATH
    CMAKE_ARGS -DOPENCL_ROOT:PATH=${OPENCL_ROOT}
        -DCMAKE_INSTALL_PREFIX:PATH=${CMAKE_BINARY_DIR}/clfft_install
    INSTALL_DIR ${CMAKE_BINARY_DIR}/clfft_install
)

file(GLOB BenchmarkSources *.cpp)
set(FFTW_INSTALL_DIR "~/fftw_install" CACHE PATH "FFTW install folder")
ExternalProject_Get_Property(clfft install_dir)
include_directories(${OPENCL_ROOT}/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/clfft/src/include)
include_directories(${install_dir}/include)
include_directories(${FFTW_INSTALL_DIR}/include)
add_library(Benchmark STATIC ${BenchmarkSources} clfft)
add_dependencies(Benchmark clfft)