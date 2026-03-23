cmake_minimum_required(VERSION 3.10)
project(deqalloc LANGUAGES CXX)

# Set C++17 standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Enable optimization with debugging symbols
set(CMAKE_BUILD_TYPE RelWithDebInfo)
# set(CMAKE_BUILD_TYPE Debug)

# Generate position-independent code; required for shared libraries on many platforms
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_compile_options(-ftls-model=initial-exec)

# Include directories
include_directories(.)

#include heap layers
include(FetchContent)
FetchContent_Declare(
  heaplayers
  GIT_REPOSITORY https://github.com/emeryberger/Heap-Layers.git
  GIT_TAG        6e26e72
)
FetchContent_MakeAvailable(heaplayers)
include_directories(${heaplayers_SOURCE_DIR}
  ${heaplayers_SOURCE_DIR}/utility
  ${heaplayers_SOURCE_DIR}/heaps)

include(FetchContent)
FetchContent_Declare(
  printf
  GIT_REPOSITORY https://github.com/emeryberger/printf.git
  GIT_TAG        master
)
FetchContent_MakeAvailable(printf)
include_directories(${printf_SOURCE_DIR})

set(UNIX_SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/deqalloc/wrappers/gnuwrapper.cpp
  libdeqalloc.cpp
  ${printf_SOURCE_DIR}/printf.cpp
)

set(MACOS_SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/deqalloc/wrappers/macwrapper.cpp
  libdeqalloc.cpp
  ${printf_SOURCE_DIR}/printf.cpp
)

if(APPLE)
  set(ALL_SOURCES ${MACOS_SOURCES})
else()
  set(ALL_SOURCES ${UNIX_SOURCES})
endif()

#Compile time definitions for shared library
set(DEFINES "")
if(DEFAULT_LIST_BYTES)
  message(STATUS "Thread-local pool size is ${DEFAULT_LIST_BYTES} bytes")
  set(DEFINES "${DEFINES} DEFAULT_LIST_BYTES=${DEFAULT_LIST_BYTES}")
endif()

# Create shared library
add_library(deqalloc SHARED ${ALL_SOURCES})
target_compile_definitions(deqalloc PUBLIC -DNDEBUG -D_REENTRANT=1 ${DEFINES})

add_library(deqalloc_localseglist SHARED ${ALL_SOURCES})
target_compile_definitions(deqalloc_localseglist PUBLIC -DNDEBUG -D_REENTRANT=1 -DLOCAL_SEGLIST ${DEFINES})

add_library(deqalloc_genericdeque SHARED ${ALL_SOURCES})
target_compile_definitions(deqalloc_genericdeque PUBLIC -DNDEBUG -D_REENTRANT=1 -DFC_DEQUE ${DEFINES})

# Let CMake handle platform-specific library settings
set_target_properties(deqalloc PROPERTIES 
    VERSION 1.0.0
    SOVERSION 1)

set_target_properties(deqalloc_genericdeque PROPERTIES
  VERSION 1.0.0
  SOVERSION 1)

# Add threading support if needed
find_package(Threads)
if(Threads_FOUND)
    target_link_libraries(deqalloc Threads::Threads)
  target_link_libraries(deqalloc_genericdeque Threads::Threads)
  target_link_libraries(deqalloc_localseglist Threads::Threads)
endif()

#NOTE: Uncomment the following line in case you need to link statically
#target_link_libraries(deqalloc dl)

#Conditionally compile unit tests if UNIT_TESTS is set
if(UNIT_TESTS)
  #include rapidcheck unit test library
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
endif()

add_subdirectory(benchmarks)
