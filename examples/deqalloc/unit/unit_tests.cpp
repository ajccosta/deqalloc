#include <cstdlib>
#include <iostream>
#include <rapidcheck.h>
#include <thread>
#include <string>
#include <queue>
#include <functional>


#include "heaplayers.h"

#include "structures/HarrisLinkedListTest.hpp"

//Ad-hoc thread pool
#define test_f_inp unsigned short, size_t
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
HeapType * getCustomHeap() {
  static char thBuf[sizeof(HeapType)];
  static HeapType * th = new (thBuf) HeapType;
  return th;
}

constexpr size_t max_sz = 1*1024*1024 / 4;
constexpr std::array<char, max_sz> make_filled_buffer(char ch) {
  std::array<char, max_sz> arr{}; for (auto& c : arr) { c = ch; } return arr;
}
//Statically filled buffer
static std::array<char, max_sz> buf = make_filled_buffer('a');

//The test function
template<class HeapType>
void testHeap(unsigned short n, size_t sz) {
  assert(sz <= max_sz);
  auto heap = getCustomHeap<HeapType>();
  using type = char;
  std::vector<type*> ptrs_to_free(n);
  for(int i = 0; i < n; i++) {
    type* ptr = (type*)heap->malloc(sz);
    ptrs_to_free[i] = ptr;
    RC_ASSERT((uintptr_t)ptr != 0);
    memcpy(ptr, buf.data(), sz);
  }
  for(int i = 0; i < n; i++) {
    heap->free(ptrs_to_free[i]);
  }
}

template<class MultiAllocHeapType>
void testMultiAllocHeap(unsigned short n, uint8_t n_sim_allocs) {
  auto heap = getCustomHeap<MultiAllocHeapType>();
  using type = char;
  using node_t = typename MultiAllocHeapType::node_t;
  size_t sz = 8; //always use the same size
  std::vector<std::tuple<node_t*, node_t*, node_t*>> ptrs_to_free(n);
  for(int i = 0; i < n; i++) {
    auto [start, end] = heap->malloc(sz, n_sim_allocs);
    RC_ASSERT((uintptr_t)start != 0 && (uintptr_t)end != 0);
    node_t* next = start;
    size_t allocated = 1;
    while(next != end) { next = next->next; allocated++; }
    RC_ASSERT(allocated == n_sim_allocs);
    //Write to first node in list
    ptrs_to_free[i] = {start, start->next, end};
    memcpy(start, buf.data(), sz);
  }
  for(int i = 0; i < n; i++) {
    auto [start, second, end] = ptrs_to_free[i];
    start->next = second;
    heap->free(start, end);
  }
}

template<typename K, typename V, typename Allocator>
inline HarrisLinkedListTest<K, V, Allocator>& get_list() {
  static HarrisLinkedListTest<K, V, Allocator> list;
  return list;
}

template<class HeapType>
void testSharedList(unsigned short n, size_t ignored) {
  using k_t = decltype(n);
  using v_t = decltype(n);
  auto& list = get_list<k_t, v_t, HeapType>();
  for(k_t i = 0; i < n; i++) {
    list.add((k_t) i, (v_t) i+2);
    list.remove((k_t) i+1);
  }
}

//Heap type definitions
class SegmentHeapUT : public MiniSegHeap<18,
                                         ThreadLocalStack<SegmentHeap<>>,
                                         SegmentHeap<>> {};

class DequeHeapUT : public DequeHeap<SegmentHeap<>> {};

class DeqallocUT : public MiniSegHeap<18,
                                      ThreadLocalStack<DequeHeapUT>,
                                      SegmentHeapUT> {};

static const unsigned int num_tests = 500;
//Do <num_tests> runs in each test
static std::string RC_PARAMS = string("RC_PARAMS=max_success="+std::to_string(num_tests));

int main() {
  putenv((char*)RC_PARAMS.c_str());

  launch_thread_pool();

  rc::check("Single threaded SegmentHeap Multi Alloc", [](unsigned short n, uint8_t n_sim_allocs) {
    RC_PRE(n > 0);
    RC_PRE(n_sim_allocs > 0);
    testMultiAllocHeap<SegmentHeap<>>(n, n_sim_allocs);
  });

  rc::check("Multi threaded SegmentHeap Multi Alloc", [](unsigned short n, uint8_t n_sim_allocs) {
    RC_PRE(n > 0);
    RC_PRE(n_sim_allocs > 0);
    auto f = [](unsigned short n, uint8_t n_sim_allocs) { testMultiAllocHeap<SegmentHeap<>>(n, n_sim_allocs); };
    run_multi_threaded(nthreads, f, n/nthreads + n%nthreads, n_sim_allocs);
  });

  rc::check("Multi threaded SegmentHeap", [](unsigned short n, size_t sz) {
    RC_PRE(n > 0);
    sz %= max_sz;
    RC_PRE(sz > 0);
    auto f = [](unsigned short n, size_t sz) {
      testHeap<SegmentHeapUT>(n, sz);
    };
    run_multi_threaded(nthreads, f, n, sz);
  });

  rc::check("Single threaded DequeHeap Multi Alloc", [](unsigned short n) {
    RC_PRE(n > 0);
    uint8_t n_sim_allocs = 4; //In DequeHeap, the lists are always the same size
    testMultiAllocHeap<DequeHeapUT>(n, n_sim_allocs);
  });

  rc::check("Multi threaded DequeHeap Multi Alloc", [](unsigned short n) {
    RC_PRE(n > 0);
    uint8_t n_sim_allocs = 4; //In DequeHeap, the lists are always the same size
    auto f = [](unsigned short n, uint8_t n_sim_allocs) { testMultiAllocHeap<DequeHeapUT>(n, n_sim_allocs); };
    run_multi_threaded(nthreads, f, n/nthreads + n%nthreads, n_sim_allocs);
  });

  rc::check("Single threaded Deqalloc", [](unsigned short n, size_t sz) {
    RC_PRE(n > 0);
    RC_PRE(sz > 0);
    sz %= max_sz;
    RC_PRE(SegmentHeapUT::getMaxNumObjects(sz) >= DeqallocUT::list_length);
    testHeap<DeqallocUT>(n, sz);
  });

  rc::check("Multi threaded Deqalloc", [](unsigned short n, size_t sz) {
    RC_PRE(n > 0);
    RC_PRE(sz > 0);
    sz %= max_sz;
    RC_PRE(SegmentHeapUT::getMaxNumObjects(sz) >= DeqallocUT::list_length);
    testHeap<DeqallocUT>(n, sz);
    auto f = [](unsigned short n, size_t sz) { testHeap<DeqallocUT>(n, sz); };
    run_multi_threaded(nthreads, f, n/nthreads + n%nthreads, sz);
  });

  //rc::check("Harris Linked List Deqalloc", [](unsigned short n) {
  //  RC_PRE(n > 0);
  //  auto f = [](unsigned short n, size_t ignore) { testSharedList<DeqallocUT>(n, 0); };
  //  run_multi_threaded(nthreads, f, n/nthreads + n%nthreads, 0);
  //});

  join_thread_pool();
}
