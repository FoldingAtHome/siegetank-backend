cmake_minimum_required (VERSION 2.6)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Valid Options: Debug, Release, RelWithDebIfno" FORCE)
endif()

file(GLOB CoreSources "src/*.cpp")
file(GLOB OpenMMCoreSources "src/openmm/*.cpp")
set(OpenMMCoreSources ${OpenMMCoreSources} ${CoreSources})
file(GLOB TeraChemCoreSource "src/terachem/*.cpp")

set(POCO_INSTALL_DIR "~/poco152_install" CACHE FILEPATH "Poco 1.5.2 install folder")
set(OPENMM_INSTALL_DIR "~/openmm_install" CACHE FILEPATH "OpenMM 6.0 install folder")
set(FFTW_INSTALL_DIR "~/fftw_install" CACHE FILEPATH "FFTW install folder")

find_package(OpenSSL)
find_package(Threads)

include_directories("${PROJECT_SOURCE_DIR}/include")
include_directories("${PROJECT_SOURCE_DIR}/include/openmm")
include_directories("${POCO_INSTALL_DIR}/include")
include_directories("${OPENMM_INSTALL_DIR}/include")
include_directories("${FFTW_INSTALL_DIR}/include")

set(CORE_VERSION 1)
add_definitions(-DCORE_VERSION=${CORE_VERSION})

link_directories(${POCO_INSTALL_DIR}/lib)
set(poco_libraries PocoNetSSL PocoCrypto PocoUtil PocoJSON PocoXML PocoNet PocoFoundation)
link_directories(${OPENMM_INSTALL_DIR}/lib)
link_directories(${OPENMM_INSTALL_DIR}/lib/plugins)
set(openmm_libraries OpenMMCPU_static OpenMM_static)

if(FFTW_INSTALL_DIR)
    set(openmm_pme_plugin OpenMMPME_static)
    link_directories(${FFTW_INSTALL_DIR}/lib)
    set(fftw_libraries fftw3f fftw3f_threads)
    add_definitions(-DUSE_FFTW)
endif(FFTW_INSTALL_DIR)

enable_testing()
add_subdirectory(tests)

add_definitions(-DOPENMM_CPU)
add_executable(ocore src/binary/ocore.cpp ${OpenMMCoreSources})
target_link_libraries(ocore ${poco_libraries} ${openmm_libraries} ${openmm_pme_plugin} ${fftw_libraries} ${OPENSSL_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
