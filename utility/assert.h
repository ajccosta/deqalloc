#ifndef ASSERT_H
#define ASSERT_H

#include <cstdlib>
#include <iostream>

#ifndef NDEBUG
#define deq_assert(cond) \
    do { \
        if (!(cond)) { \
            std::cerr << "Assertion failed: (" #cond "), file " << __FILE__ \
                      << ", line " << __LINE__ << std::endl; \
            std::abort(); \
        } \
    } while (0)
#else
#define deq_assert(cond)
#endif

#endif