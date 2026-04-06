cmake_minimum_required(VERSION 3.14)
project(wimage
    VERSION 1.0
    LANGUAGES C
    DESCRIPTION "Clean-room Windows Imaging Format (WIM) reader/writer")

set(CMAKE_C_STANDARD 11)

option(ENABLE_TESTS "Build GTest unit tests" OFF)

# Bake project version into a generated header. The major/minor come from
# project(... VERSION X.Y); each release bumps major by +1.0 (v1.0 -> v2.0
# -> v3.0 ...), so the number visible in --version and "wimage info" output
# tracks the release line directly.
configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/libwim/wimage_version.h.in
    ${CMAKE_CURRENT_BINARY_DIR}/generated/wimage_version.h
    @ONLY)

# --- libwim static library ---
file(GLOB LIBWIM_SRCS libwim/*.c)
add_library(wim_static STATIC ${LIBWIM_SRCS})
target_include_directories(wim_static PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/libwim)
target_compile_definitions(wim_static PRIVATE _POSIX_C_SOURCE=200809L _DEFAULT_SOURCE)

find_package(Threads REQUIRED)
find_package(OpenSSL QUIET COMPONENTS Crypto)
if(OpenSSL_FOUND)
    target_compile_definitions(wim_static PRIVATE HAVE_OPENSSL_SHA1=1)
    target_link_libraries(wim_static PRIVATE OpenSSL::Crypto)
endif()

# --- wimage CLI tool ---
add_executable(wimage wimage.c)
target_include_directories(wimage PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/libwim
    ${CMAKE_CURRENT_BINARY_DIR}/generated)
target_link_libraries(wimage PRIVATE wim_static Threads::Threads)

# --- GTest unit tests ---
if(ENABLE_TESTS)
    set(CMAKE_CXX_STANDARD 17)
    enable_language(CXX)
    include(FetchContent)
    FetchContent_Declare(googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG v1.14.0)
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)

    enable_testing()
    file(GLOB TEST_SRCS tests/test_*.cpp)
    add_executable(wimage_tests ${TEST_SRCS})
    target_include_directories(wimage_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/libwim)
    target_link_libraries(wimage_tests PRIVATE wim_static GTest::gtest_main Threads::Threads)

    include(GoogleTest)
    gtest_discover_tests(wimage_tests)
endif()
