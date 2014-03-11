# ########################################################################
# Copyright 2013 Advanced Micro Devices, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ########################################################################


# Locate the POCO (http://www.POCO.org/) Framework.
#
# Defines the following variables:
#
#   POCO_FOUND - Found the POCO framework
#   POCO_INCLUDE_DIRS - Include directories
#
# Also defines the library variables below as normal
# variables.  These contain debug/optimized keywords when
# a debugging library is found.
#
#   POCO_LIBRARIES - libPOCO3f libPOCO3f_threads
#
# Accepts the following variables as input:
#
#   POCO_ROOT - (as a CMake or environment variable)
#                The root directory of the POCO install prefix
#
#-----------------------
# Example Usage:
#
#    find_package(POCO REQUIRED)
#    include_directories(${POCO_INCLUDE_DIRS})
#
#    add_executable(foo foo.cc)
#    target_link_libraries(foo ${POCO_LIBRARIES})
#
#-----------------------
if(DEFINED ENV{POCO_ROOT})
    set(POCO_ROOT $ENV{POCO_ROOT} CACHE PATH "Environment variable defining the root of POCO")
else()
    set(POCO_ROOT "$ENV{HOME}/poco152_install" CACHE PATH "Environment variable defining the root of POCO")
endif()

set(POCO_LIBRARY_NAMES PocoNetSSL PocoCrypto PocoUtil PocoJSON PocoXML PocoNet PocoFoundation)

foreach(POCO_LIBRARY_NAME ${POCO_LIBRARY_NAMES})
    if(UNIX)
        set(POCO_STATIC_LIB "lib${POCO_LIBRARY_NAME}.a")
    endif()
    find_library(POCO_LIB_ID_${POCO_LIBRARY_NAME}
        NAMES ${POCO_STATIC_LIB}
        HINTS
            ${POCO_ROOT}
            ${POCO_ROOT}/lib
            $ENV{POCO_ROOT}
            $ENV{POCO_ROOT}/lib
        DOC "POCO static library"
    )
    set(POCO_LIBRARIES ${POCO_LIB_ID_${POCO_LIBRARY_NAME}} ${POCO_LIBRARIES})
endforeach()
mark_as_advanced(POCO_LIBRARIES)

find_path(POCO_INCLUDE_DIRS
    NAMES Poco/Poco.h
    HINTS
        ${POCO_ROOT}/api
        ${POCO_ROOT}/include
        ${POCO_ROOT}
        $ENV{POCO_ROOT}/api
        $ENV{POCO_ROOT}/include
        $ENV{POCO_ROOT}
)
mark_as_advanced(POCO_INCLUDE_DIRS)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(POCO DEFAULT_MSG POCO_LIBRARIES POCO_INCLUDE_DIRS)

if( NOT POCO_FOUND )
    message(STATUS "FindPOCO looked for single precision libraries named: POCO3f or libPOCO3f-3")
    message(STATUS "FindPOCO looked for double precision libraries named: POCO3 or libPOCO3-3")
endif()
