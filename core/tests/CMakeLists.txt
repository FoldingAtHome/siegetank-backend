link_directories(${Core_BINARY_DIR})

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${Core_BINARY_DIR})


set(test_progs "test_core.cpp")
file(GLOB poco_libs ${POCO_INSTALL_DIR}/lib/*.a)

link_directories(${POCO_INSTALL_DIR}/lib)
set(poco_libraries PocoNetSSL PocoCrypto PocoUtil PocoJSON PocoXML PocoNet PocoFoundation)

# base library tests
foreach(test_prog ${test_progs})
    get_filename_component(test_root ${test_prog} NAME_WE)
    add_executable(${test_root} ${test_prog} ${CoreSources})
    target_link_libraries(${test_root} ${poco_libraries} ${OPENSSL_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})    
    add_test(${test_root} ${Core_BINARY_DIR}/${test_root})
endforeach()

link_directories(${OPENMM_INSTALL_DIR}/lib)
link_directories(${OPENMM_INSTALL_DIR}/lib/plugins)
set(openmm_libraries OpenMMCPU_static OpenMM_static)

set(test_openmm_progs "test_openmm_core.cpp")
foreach(test_prog ${test_openmm_progs})
    get_filename_component(test_root ${test_prog} NAME_WE)
    add_executable(${test_root} ${test_prog} ${CoreSources} ${OpenMMCoreSources})
    target_link_libraries(${test_root} ${poco_libraries} ${openmm_libraries} ${OPENSSL_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})    
    add_test(${test_root} ${Core_BINARY_DIR}/${test_root})
endforeach()
# base 