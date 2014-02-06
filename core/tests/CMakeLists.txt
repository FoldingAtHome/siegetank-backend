link_directories(${Core_BINARY_DIR})

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${Core_BINARY_DIR})
set(CORE_VERSION 1)

file(GLOB test_progs "test*.cpp")
file(GLOB poco_libs ${POCO_INSTALL_DIR}/lib/*.a)

link_directories(${POCO_INSTALL_DIR}/lib)
add_definitions(-DCORE_VERSION=${CORE_VERSION})
set(poco_libraries PocoNetSSL PocoCrypto PocoUtil PocoJSON PocoXML PocoNet PocoFoundation)

foreach(test_prog ${test_progs})
    get_filename_component(test_root ${test_prog} NAME_WE)
    add_executable(${test_root} ${test_prog} ${CppFiles})
    target_link_libraries(${test_root} ${poco_libraries} ${OPENSSL_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})    
    add_test(${test_root} ${Core_BINARY_DIR}/${test_root})
endforeach(test_prog ${test_progs})