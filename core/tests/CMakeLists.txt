# test_benchmark.cpp
if(BUILD_BENCHMARK)
    add_executable(test_benchmark test_benchmark.cpp)

    include_directories(${OPENCL_INCLUDE_DIRS})
    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../benchmark)
    include_directories(${CORE_ROOT_DIR}/clfft_install/include)
    include_directories(${FFTW_INCLUDE_DIRS})
    if(UNIX)
        set(CLFFT_LIB_PATH ${CORE_ROOT_DIR}/clfft_install/lib64/import/libclFFT.a)
    endif()
    link_directories(${FFTW_INSTALL_DIR}/lib)
    target_link_libraries(test_benchmark Benchmark ${OPENCL_LIBRARIES} ${CLFFT_LIB_PATH} ${OPENCL_LIBRARIES} ${FFTW_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})

    STRING(FIND ${OPENCL_INCLUDE_DIRS} "intel" IS_INTEL)
    STRING(FIND ${OPENCL_INCLUDE_DIRS} "Framework" APPLE_OPENCL)
    if(${IS_INTEL} GREATER -1 OR ${APPLE_OPENCL} GREATER -1)
        message(STATUS '----TRUE' ${IS_INTEL} ${APPLE_OPENCL})
        add_test(test_benchmark test_benchmark 0 1 )
    else()
        message(STATUS '----FALSE' ${IS_INTEL} ${APPLE_OPENCL})
        add_test(test_benchmark test_benchmark 0 0)
    endif()

endif()

add_executable(test_core test_core.cpp)
target_link_libraries(test_core Core ${OPENSSL_LIBRARIES} ${POCO_LIBRARIES} ${OPENSSL_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
add_test(test_core test_core)

# link_directories(${Core_BINARY_DIR})
# 
# # test using the CPU platform
# add_definitions(-DOPENMM_CPU)
# set(openmm_libraries OpenMMCPU_static ${openmm_libraries})
# 
# set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${Core_BINARY_DIR})
# set(test_progs "test_core.cpp")
# file(GLOB poco_libs ${POCO_INSTALL_DIR}/lib/*.a)
# 
# # base library tests
# foreach(test_prog ${test_progs})
#     get_filename_component(test_root ${test_prog} NAME_WE)
#     add_executable(${test_root} ${test_prog} ${CoreSources})
#     target_link_libraries(${test_root} ${poco_libraries} ${OPENSSL_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})    
#     add_test(${test_root} ${Core_BINARY_DIR}/${test_root})
# endforeach()
#
