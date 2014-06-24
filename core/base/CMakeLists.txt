cmake_minimum_required (VERSION 2.8)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Valid Options: Debug, Release, RelWithDebIfno" FORCE)
endif()

find_package(Threads REQUIRED)

file(GLOB CoreSources "*.cpp")
#set(OPENSSL_ROOT_DIR /home/yutong/openssl_install CACHE STRING "Path to root of static OpenSSL libraries")
#set(CMAKE_PREFIX_PATH "/home/yutong/openssl_install")
#find_package(OpenSSL)

# if(WIN32)
#   set(OPENSSL_CRYPTO_STATIC /home/yutong/openssl_install/libcrypto.a CACHE STRING "Static library")
#   set(OPENSSL_SSL_STATIC /home/yutong/openssl_install/libssl.a CACHE STRING "Static library")
# else()
#   set(OPENSSL_CRYPTO_STATIC /home/yutong/openssl_install/libcrypto.a CACHE STRING "Static library")
#   set(OPENSSL_SSL_STATIC /home/yutong/openssl_install/libssl.a CACHE STRING "Static library")
# endif()

# set(OPENSSL_LIBRARIES ${OPENSSL_SSL_STATIC} ${OPENSSL_CRYPTO_STATIC})

add_library(Core ${CoreSources})
