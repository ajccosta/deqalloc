#include <cstdlib>
#include <iostream>
#include <rapidcheck.h>
#include <thread>
#include <string>

#include "heaplayers.h"

//Utility functions
template<class... Inputs>
void run_multi_threaded(auto f,
  Inputs... ins,
  size_t nthreads = thread::hardware_concurrency()) {

  std::vector<thread> threads(nthreads);
  for (int i = 0; i < nthreads; i++) threads[i] = thread(f, ins...);
  for (int i = 0; i < nthreads; i++) threads[i].join();
}

template<class HeapType>
inline static HeapType * getCustomHeap() {
  static char thBuf[sizeof(HeapType)];
  static HeapType * th = new (thBuf) HeapType;
  return th;
}

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

static const unsigned int num_tests = 5000;
//Do <num_tests> runs in each test
static std::string RC_PARAMS = string("RC_PARAMS=max_success="+std::to_string(num_tests));

int main() {
  putenv((char*)RC_PARAMS.c_str());

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
    auto f = [&](unsigned short n){
      testHeap<SegmentHeapUT>(n);
    };
    auto nthreads = thread::hardware_concurrency();
    run_multi_threaded<unsigned short>(f, n/nthreads, nthreads);
  });
}
