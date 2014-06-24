set(TEST_DEPENDENCIES ${OPENSSL_LIBRARIES} ${POCO_LIBRARIES} ${OPENSSL_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})

if(WIN32)
	set(TEST_DEPENDENCIES ${TEST_DEPENDENCIES} dl)
endif()

add_executable(test_core test_core.cpp)
target_link_libraries(test_core Core ${TEST_DEPENDENCIES})

add_test(test_core test_core)

add_executable(test_poco test_poco.cpp)
target_link_libraries(test_poco Core ${TEST_DEPENDENCIES})
add_test(test_poco test_poco)

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
