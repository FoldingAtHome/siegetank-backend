if(BUILD_BENCHMARK)
    add_executable(test_benchmark test_benchmark.cpp)
    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../benchmark)
    include_directories(${CORE_ROOT_DIR}/clfft_install/include)
    include_directories(${FFTW_INCLUDE_DIRS})
    if(APPLE)
        set(CLFFT_LIB_PATH ${CORE_ROOT_DIR}/clfft_install/lib64/import/libclFFT.a)
    endif()
    link_directories(${FFTW_INSTALL_DIR}/lib)
    target_link_libraries(test_benchmark ${CLFFT_LIB_PATH} ${OPENCL_LIBRARY} ${FFTW_LIBRARIES} Benchmark)
endif()

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
# set(test_openmm_progs "test_openmm_core.cpp")
# foreach(test_prog ${test_openmm_progs})
#    get_filename_component(test_root ${test_prog} NAME_WE)
#     add_executable(${test_root} ${test_prog} ${OpenMMCoreSources})
#     target_link_libraries(${test_root} ${poco_libraries} ${openmm_libraries} ${OPENSSL_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
#     add_test(${test_root} ${Core_BINARY_DIR}/${test_root})
# endforeach()