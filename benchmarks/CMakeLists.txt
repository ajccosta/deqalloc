#set binary output dir
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/benchmarks/flock)

#flock benchmarks
include(FetchContent)
FetchContent_Declare(
  flock
  GIT_REPOSITORY https://github.com/ajccosta/flock-private.git
  GIT_TAG        transactions
  EXCLUDE_FROM_ALL #dont build by default
)
FetchContent_MakeAvailable(flock)
# Collect all flock benchmark targets into build_flock
add_custom_target(build_flock
  DEPENDS
    arttree_lck
    btree_lck
    hash_block_lck
    hash_list_lck
    leaftree_lck
    list_lck
    skiplist_lck
  COMMENT "Building flock binaries..."
)

#setbench benchmarks
FetchContent_Declare(
  setbench
  GIT_REPOSITORY https://github.com/ajccosta/amortizedfree-setbench.git
  GIT_TAG        main
)
FetchContent_Populate(setbench)
add_custom_target(build_setbench
  COMMAND bash compile.sh
  COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/benchmarks/setbench
  COMMAND ${CMAKE_COMMAND} -E copy_directory
    ${setbench_SOURCE_DIR}/microbench/bin
    ${CMAKE_BINARY_DIR}/benchmarks/setbench
  WORKING_DIRECTORY ${setbench_SOURCE_DIR}/microbench
  COMMENT "Building and copying setbench binaries..."
)

#build other allocators
set(ALLOCATOR_LIB_DIR ${CMAKE_BINARY_DIR}/allocators)
add_custom_target(build_allocators
  COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/build_allocators.sh
  COMMAND ${CMAKE_COMMAND} -E create_symlink
    ${CMAKE_BINARY_DIR}/libdeqalloc.so
    ${ALLOCATOR_LIB_DIR}/libdeqalloc.so
  COMMAND ${CMAKE_COMMAND} -E create_symlink
    ${CMAKE_BINARY_DIR}/libdeqalloc_localseglist.so
    ${ALLOCATOR_LIB_DIR}/libdeqalloc_localseglist.so
  COMMAND ${CMAKE_COMMAND} -E create_symlink
    ${CMAKE_BINARY_DIR}/libdeqalloc_genericdeque.so
    ${ALLOCATOR_LIB_DIR}/libdeqalloc_genericdeque.so
  BYPRODUCTS
    ${ALLOCATOR_LIB_DIR}/libjemalloc.so
    ${ALLOCATOR_LIB_DIR}/libmesh.so
    ${ALLOCATOR_LIB_DIR}/libmimalloc.so
    ${ALLOCATOR_LIB_DIR}/libhoard.so
    ${ALLOCATOR_LIB_DIR}/libscalloc.so
    ${ALLOCATOR_LIB_DIR}/libtcmalloc.so
    ${ALLOCATOR_LIB_DIR}/libsnmalloc.so
    ${ALLOCATOR_LIB_DIR}/liblockfree.so
    ${ALLOCATOR_LIB_DIR}/librpmalloc.so
    ${ALLOCATOR_LIB_DIR}/libtbbmalloc.so
  WORKING_DIRECTORY ${ALLOCATOR_LIB_DIR}/..
  COMMENT "Building third-party allocators..."
)

set_target_properties(build_allocators PROPERTIES EXCLUDE_FROM_ALL TRUE)


#umbrella target for all benchmarks and allocators
add_custom_target(benchmarks
  DEPENDS build_flock build_setbench build_allocators deqalloc
  COMMENT "Building all benchmarks..."
)

#exclude both from the default build target
set_target_properties(
  build_flock
  build_setbench
  build_allocators
  benchmarks
  PROPERTIES
  EXCLUDE_FROM_ALL TRUE
)