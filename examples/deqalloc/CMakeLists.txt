cmake_minimum_required(VERSION 3.10)
project(deqalloc LANGUAGES CXX)

# Set C++14 standard
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Enable optimization with debugging symbols
set(CMAKE_BUILD_TYPE RelWithDebInfo)
# set(CMAKE_BUILD_TYPE Debug)

# Generate position-independent code; required for shared libraries on many platforms
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Common definitions
add_definitions(-DNDEBUG -D_REENTRANT=1)

# Include directories
include_directories(. ../.. ../../util)

include(FetchContent)
FetchContent_Declare(
  printf
  GIT_REPOSITORY https://github.com/emeryberger/printf.git
  GIT_TAG        master
)
FetchContent_MakeAvailable(printf)
include_directories(${printf_SOURCE_DIR})

set(UNIX_SOURCES
  ../../wrappers/gnuwrapper.cpp
  libdeqalloc.cpp
  ${printf_SOURCE_DIR}/printf.cpp
)

set(MACOS_SOURCES
  ../../wrappers/macwrapper.cpp
  libdeqalloc.cpp
  ${printf_SOURCE_DIR}/printf.cpp
)

if(APPLE)
  set(ALL_SOURCES ${MACOS_SOURCES})
else()
  set(ALL_SOURCES ${UNIX_SOURCES})
endif()

# Create shared library
add_library(deqalloc SHARED ${ALL_SOURCES})

# Let CMake handle platform-specific library settings
set_target_properties(deqalloc PROPERTIES 
    VERSION 1.0.0
    SOVERSION 1)

# Add threading support if needed
find_package(Threads)
if(Threads_FOUND)
    target_link_libraries(deqalloc Threads::Threads)
endif()

include(FetchContent)
FetchContent_Declare(
  rapidcheck
  GIT_REPOSITORY https://github.com/emil-e/rapidcheck.git
  GIT_TAG        master
)
FetchContent_MakeAvailable(rapidcheck)
target_compile_options(rapidcheck PUBLIC -fconcepts)
include_directories(${rapidcheck_SOURCE_DIR})
add_subdirectory(unit)
