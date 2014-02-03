link_directories(${Core_BINARY_DIR})

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${Core_BINARY_DIR})

file(GLOB test_progs "test*.cpp")
file(GLOB poco_libs ${POCO_INSTALL_DIR}/lib/*)
foreach(test_prog ${test_progs})
    get_filename_component(test_root ${test_prog} NAME_WE)
    add_executable(${test_root} ${test_prog} ${CppFiles})
    target_link_libraries(${test_root} ${poco_libs} ${OPENSSL_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
    add_test(${test_root} ${Core_BINARY_DIR}/${test_root})
endforeach(test_prog ${test_progs})