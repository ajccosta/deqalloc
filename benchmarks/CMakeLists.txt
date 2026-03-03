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

#umbrella target for all benchmarks#
add_custom_target(benchmarks
  DEPENDS build_flock build_setbench
  COMMENT "Building all benchmarks..."
)

#exclude both from the default build target
set_target_properties(build_flock build_setbench benchmarks PROPERTIES
  EXCLUDE_FROM_ALL TRUE
)