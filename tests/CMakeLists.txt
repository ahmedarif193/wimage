cmake_minimum_required(VERSION 3.14)
project(wimage_tests C CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_C_STANDARD 11)

include(FetchContent)
FetchContent_Declare(googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.14.0)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# Build libwim sources as a C static library
set(LIBWIM_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../libwim)
file(GLOB LIBWIM_SRCS ${LIBWIM_DIR}/*.c)
add_library(wim_static STATIC ${LIBWIM_SRCS})
target_include_directories(wim_static PUBLIC ${LIBWIM_DIR})
target_compile_definitions(wim_static PRIVATE _POSIX_C_SOURCE=200809L _DEFAULT_SOURCE)

# Find pthreads
find_package(Threads REQUIRED)

# Test executable
enable_testing()
file(GLOB TEST_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/test_*.cpp)
add_executable(wimage_tests ${TEST_SRCS})
target_include_directories(wimage_tests PRIVATE ${LIBWIM_DIR})
target_link_libraries(wimage_tests PRIVATE wim_static GTest::gtest_main Threads::Threads)

include(GoogleTest)
gtest_discover_tests(wimage_tests)
