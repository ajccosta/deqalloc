#include <cstdlib>
#include <iostream>
#include <rapidcheck.h>
#include <thread>
#include <string>
#include <queue>
#include <functional>

#include "heaplayers.h"

//Ad-hoc thread pool
#define test_f_inp unsigned short
using test_f_out = void;

using test_f_type = test_f_out(test_f_inp);
constexpr size_t nthreads = 64;
std::vector<thread> threads(nthreads);
std::queue<std::pair<std::function<test_f_type>,std::tuple<test_f_inp>>> workq;
std::atomic<int> running {0};
std::atomic<bool> term {false};
std::mutex thread_pool_lock;

void thread_pool_loop(size_t tid) {
  while(true) {
    thread_pool_lock.lock();
    if(term) {
      thread_pool_lock.unlock();
      break;
    }
    if(!workq.empty()) {
      auto [func, args] = workq.front();
      workq.pop();
      thread_pool_lock.unlock();
      std::apply(func, args);
      running--;
    } else {
      thread_pool_lock.unlock();
    }
  }
}

void launch_thread_pool() {
  thread_pool_lock.lock();
  for (int i = 0; i < nthreads; i++) threads[i] = thread(thread_pool_loop, i);
}

void join_thread_pool() {
  term = true;
  thread_pool_lock.unlock();
  for (int i = 0; i < nthreads; i++) threads[i].join();
}

//Utility functions
template<class Thunk, class... Args>
void run_multi_threaded(size_t numthreads, Thunk f, Args... args) {
  //Assumes lock is held when called
  assert(numthreads <= nthreads);
  for(int i = 0; i < numthreads; i++) {
    workq.push({f, {args...}});
  }
  running = numthreads;
  thread_pool_lock.unlock();
  while(running > 0) { std::this_thread::sleep_for (std::chrono::microseconds(100)); }
  thread_pool_lock.lock();
}

template<class HeapType>
inline static HeapType * getCustomHeap() {
  static char thBuf[sizeof(HeapType)];
  static HeapType * th = new (thBuf) HeapType;
  return th;
}

//The test function
template<class HeapType>
void testHeap(unsigned short n) {
  auto heap = getCustomHeap<HeapType>();
  size_t sz = 24;
  using type = size_t;
  std::vector<type*> ptrs_to_free(n);
  for(int i = 0; i < n; i++) {
    type* ptr = (type*)heap->malloc(sz);
    ptrs_to_free[i] = ptr;
    RC_ASSERT((uintptr_t)ptr != 0);
    ptr[0] = 0xDEADBEEF;
  }
  for(int i = 0; i < n; i++) {
    heap->free(ptrs_to_free[i]);
  }
}

//Heap type definitions
class TopHeap : public
        UniqueHeap<
          LockedHeap<
            PosixLockType,
            SizeHeap<
              ZoneHeap<
                MmapHeap,
                65536>>>> {};

class BoundedFreeListHeapUT : public
        BoundedFreeListHeap<
          512,
          SizeHeap<
            ZoneHeap<MmapHeap, 65536>>> {};

class DeqallocUT : public
        ANSIWrapper<
          ThreadSpecificHeap<
            KingsleyHeap<
              AdaptHeap<DLList, TopHeap>,
              TopHeap>>> {};

class TwoListHeapUT : public
        ANSIWrapper<
          TwoListHeap<
            512,
            ListHeap<
              512,
              SizeHeap<
                ZoneHeap<MmapHeap, 65536>>>>> {};

class SegmentHeapUT : public SegmentHeap<> {};

static const unsigned int num_tests = 500;
//Do <num_tests> runs in each test
static std::string RC_PARAMS = string("RC_PARAMS=max_success="+std::to_string(num_tests));

int main() {
  putenv((char*)RC_PARAMS.c_str());

  launch_thread_pool();

  //rc::check("Single threaded malloc free", [](unsigned short n) {
  //  RC_PRE(n > 0);
  //  for(int i = 0; i < n; i++) {
  //    void* ptr = xxmalloc(n);
  //    xxfree(ptr);
  //    RC_ASSERT((uintptr_t)ptr != 0);
  //  }
  //});

  //rc::check("Multi threaded malloc free", [](unsigned short n) {
  //  RC_PRE(n > 0);
  //  auto f = [&](unsigned short n){
  //    for(int i = 0; i < n; i++) {
  //      void* ptr = xxmalloc(n);
  //      xxfree(ptr);
  //      RC_ASSERT((uintptr_t)ptr != 0);
  //    }
  //  };
  //  run_multi_threaded<unsigned short>(f, n);
  //});

  //rc::check("BoundedFreeListHeap", [](unsigned short n) {
  //  RC_PRE(n > 0);
  //  testHeap<BoundedFreeListHeapUT>(n);
  //});

  //rc::check("Deqalloc", [](unsigned short n) {
  //  RC_PRE(n > 0);
  //  testHeap<DeqallocUT>(n);
  //});

  //rc::check("TwoListHeap", [](unsigned short n) {
  //  RC_PRE(n > 0);
  //  testHeap<TwoListHeapUT>(n);
  //});

  //rc::check("SegmentHeap", [](unsigned short n) {
  //  RC_PRE(n > 0);
  //  testHeap<SegmentHeapUT>(n);
  //});

  rc::check("Multi threaded SegmentHeap", [](unsigned short n) {
    RC_PRE(n > 0);
    auto f = [](unsigned short n) { testHeap<SegmentHeapUT>(n); };
    run_multi_threaded(nthreads, f, n/nthreads + n%nthreads);
  });

  join_thread_pool();
}
