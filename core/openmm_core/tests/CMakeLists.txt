include_directories(${OPENMM_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../)
add_executable(test_openmm_core test_openmm_core.cpp)

target_link_libraries(test_openmm_core OpenMMCore Core ${OPENMM_CORE_DEPENDENCIES})
