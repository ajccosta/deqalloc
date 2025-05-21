#ifndef ASSERT_H
#define ASSERT_H

#ifndef NDEBUG
#define deq_assert(cond) if(!(cond)) { std::abort(); }
#else
#define deq_assert(cond)
#endif

#endif