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

find_package(Threads REQUIRED)

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


# Target directory for output symlinks
set(ALLOCATORS_DIR "${CMAKE_BINARY_DIR}/allocators")
file(MAKE_DIRECTORY "${ALLOCATORS_DIR}")

# Helper function to standardize variant target creation and symlinking
function(add_deqalloc_variant TARGET_NAME)
  cmake_parse_arguments(ARG "" "" "DEFINES" ${ARGN})
  add_library(${TARGET_NAME} SHARED ${ALL_SOURCES})
  target_include_directories(${TARGET_NAME} PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${heaplayers_SOURCE_DIR}
    ${heaplayers_SOURCE_DIR}/utility
    ${heaplayers_SOURCE_DIR}/heaps
    ${printf_SOURCE_DIR}
  )
  target_compile_definitions(${TARGET_NAME} PRIVATE NDEBUG _REENTRANT=1 ${ARG_DEFINES})
  if(DEFAULT_LIST_BYTES)
    target_compile_definitions(${TARGET_NAME} PRIVATE DEFAULT_LIST_BYTES=${DEFAULT_LIST_BYTES})
  endif()
  target_link_libraries(${TARGET_NAME} PRIVATE Threads::Threads)
  set_target_properties(${TARGET_NAME} PROPERTIES VERSION 1.0.0 SOVERSION 1)
  # Post-build symlink creation into build/allocators/
  add_custom_command(
    TARGET ${TARGET_NAME} POST_BUILD
    # Creates symlink for .so.1.0.0
    COMMAND ${CMAKE_COMMAND} -E create_symlink
      $<TARGET_FILE:${TARGET_NAME}>
      "${ALLOCATORS_DIR}/$<TARGET_FILE_NAME:${TARGET_NAME}>"
    # Creates symlink for .so (linker file)
    COMMAND ${CMAKE_COMMAND} -E create_symlink
      $<TARGET_LINKER_FILE:${TARGET_NAME}>
      "${ALLOCATORS_DIR}/$<TARGET_LINKER_FILE_NAME:${TARGET_NAME}>"
    COMMENT "Creating symlinks for ${TARGET_NAME} in allocators/"
  )
endfunction()

# Standard Allocator Variants
add_deqalloc_variant(deqalloc)
add_deqalloc_variant(deqalloc_localseglist DEFINES LOCAL_SEGLIST)
add_deqalloc_variant(deqalloc_genericdeque_fc DEFINES FC_DEQUE)
add_deqalloc_variant(deqalloc_genericdeque DEFINES LOCKING_DEQUE)
add_deqalloc_variant(deqalloc_genericdeque_localseglist_fc DEFINES FC_DEQUE LOCAL_SEGLIST)
add_deqalloc_variant(deqalloc_genericdeque_localseglist DEFINES LOCKING_DEQUE LOCAL_SEGLIST)
add_deqalloc_variant(deqalloc_remotefree_fc DEFINES REMOTE_FREE FC_DEQUE LOCAL_SEGLIST)
add_deqalloc_variant(deqalloc_remotefree DEFINES REMOTE_FREE LOCKING_DEQUE LOCAL_SEGLIST)

# Batch-size Allocator Variants
foreach(BATCH_SIZE 512 1024 2048 4096 8192)
  add_deqalloc_variant(deqalloc_${BATCH_SIZE} DEFINES DEFAULT_LIST_BYTES=${BATCH_SIZE})
  add_deqalloc_variant(deqalloc_remotefree_fc_${BATCH_SIZE} DEFINES REMOTE_FREE FC_DEQUE LOCAL_SEGLIST DEFAULT_LIST_BYTES=${BATCH_SIZE})
  add_deqalloc_variant(deqalloc_genericdeque_${BATCH_SIZE} DEFINES LOCKING_DEQUE DEFAULT_LIST_BYTES=${BATCH_SIZE})
  add_deqalloc_variant(deqalloc_remotefree_${BATCH_SIZE} DEFINES REMOTE_FREE LOCKING_DEQUE LOCAL_SEGLIST DEFAULT_LIST_BYTES=${BATCH_SIZE})
endforeach()


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
