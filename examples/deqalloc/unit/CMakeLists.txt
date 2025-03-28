add_executable(unit_tests "unit_tests.cpp")
target_link_libraries(unit_tests PRIVATE rapidcheck Threads::Threads)
target_include_directories(unit_tests PRIVATE ${PROJECT_SOURCE_DIR})
target_compile_definitions(unit_tests PRIVATE "")
