# Locate the OpenMM (http://pocoproject.org/) Framework.
#
# Defines the following variables:
#
#   OPENMM_FOUND - Found the OpenMM framework
#   OPENMM_INCLUDE_DIRS - Include directories
#
# Also defines the library variables below as normal
# variables.  These contain debug/optimized keywords when
# a debugging library is found.
#
#   OPENMM_LIBRARIES
#
# Accepts the following variables as input:
#
#   OPENMM_ROOT - (as a CMake or environment variable)
#                The root directory of the OpenMM install prefix
#
#-----------------------
# Example Usage:
#
#    find_package(OpenMM REQUIRED)
#    include_directories(${OPENMM_INCLUDE_DIRS})
#
#    add_executable(foo foo.cc)
#    target_link_libraries(foo ${OPENMM_LIBRARIES})
#
#-----------------------
if(DEFINED ENV{OPENMM_ROOT})
    set(OPENMM_ROOT $ENV{OPENMM_ROOT} CACHE PATH "Environment variable defining the root of OpenMM")
elseif(UNIX)
    set(OPENMM_ROOT "$ENV{HOME}/openmm_install" CACHE PATH "Where OpenMM is installed")
elseif(WIN32)
    set(OPENMM_ROOT "C:/Libs/openmm601" CACHE PATH "Where OpenMM is installed")    
endif()

set(OPENMM_LIBRARY_NAMES OpenMM_static OpenMMCPU_static OpenMMPME_static OpenMMCUDA_static OpenMMOpenCL_static)

foreach(OPENMM_LIBRARY_NAME ${OPENMM_LIBRARY_NAMES})
    if(UNIX)
        set(OPENMM_STATIC_LIB "lib${OPENMM_LIBRARY_NAME}.a")
    elseif(WIN32)
        set(OPENMM_STATIC_LIB "${OPENMM_LIBRARY_NAME}.lib")
    endif()
    find_library(OPENMM_LIB_ID_${OPENMM_LIBRARY_NAME}
        NAMES ${OPENMM_STATIC_LIB}
        HINTS
            ${OPENMM_ROOT}
            ${OPENMM_ROOT}/lib
            ${OPENMM_ROOT}/lib/plugins
            $ENV{OPENMM_ROOT}
            $ENV{OPENMM_ROOT}/lib
        DOC "OpenMM static library"
    )
    mark_as_advanced(OPENMM_LIB_ID_${OPENMM_LIBRARY_NAME})
    if(OPENMM_LIB_ID_${OPENMM_LIBRARY_NAME} STREQUAL OPENMM_LIB_ID_${OPENMM_LIBRARY_NAME}-NOTFOUND)
        message(STATUS "${OPENMM_STATIC_LIB} could not be found")
    else()
        set(OPENMM_LIBRARIES ${OPENMM_LIBRARIES} ${OPENMM_LIB_ID_${OPENMM_LIBRARY_NAME}})
    endif()
endforeach()

mark_as_advanced(OPENMM_LIBRARIES)

find_path(OPENMM_INCLUDE_DIRS
    NAMES OpenMM.h
    HINTS
        ${OPENMM_ROOT}/api
        ${OPENMM_ROOT}/include
        ${OPENMM_ROOT}
        $ENV{OPENMM_ROOT}/api
        $ENV{OPENMM_ROOT}/include
        $ENV{OPENMM_ROOT}
    DOC "OpenMM include directory"
)
mark_as_advanced(OPENMM_INCLUDE_DIRS)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(OpenMM DEFAULT_MSG OPENMM_LIBRARIES OPENMM_INCLUDE_DIRS)

if( NOT OPENMM_FOUND )
    message(STATUS "FindOpenMM failed to locate necessary libraries")
endif()
