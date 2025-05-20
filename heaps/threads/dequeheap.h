#ifndef DEQUE_HEAP_H
#define DEQUE_HEAP_H

#include <optional>
#include <atomic>

#include "heaps/top/segmentheap.h"
#include "threads/structures/memorystealingdeque.h"
#include "threads/threadmanager.h"
#include "utility/random.h"

template <class Super = SegmentHeap<>>
class DequeHeap : public Super {
  public:

    enum { Alignment = Super::Alignment };

    using node_t = typename Super::node_t;

  private:

    using deque_t = MemoryStealingDeque<std::tuple<node_t*, node_t*>, 5, 2>;
    deque_t deques[max_threads];

    static inline thread_local parlay::random my_rand __attribute__((tls_model ("initial-exec")));

    inline deque_t& my_deq() { return deques[thread_id()]; }
    inline deque_t& random_deq(size_t n_threads) { return deques[my_rand.rand() % n_threads]; }

  public:

    inline std::pair<node_t*, node_t*> malloc(size_t sz, size_t n) {
      auto opt = my_deq().pop_bottom();
      //Try our own deque
      if(opt) {
        auto [start_node, end_node] = opt.value();
        deq_assert(start_node != nullptr && end_node != nullptr);
        return {start_node, end_node};
      }
      //Our deque is empty, try stealing
      //Avoid atomic read of num_threads multiple times
      size_t n_threads = num_threads().load(std::memory_order_relaxed);
      size_t attempts = 0;
      while(attempts++ < n_threads) {
        //TODO re-attempt if deque is not empty?
        auto res = random_deq(n_threads).pop_top();
        auto [opt, deque_is_empty] = res;
        if(opt) {
          auto [start_node, end_node] = opt.value();
          deq_assert(start_node != nullptr && end_node != nullptr);
          return {start_node, end_node};
        }
      }
      //Could not steal either, allocate from super heap
      //  Currently, we assume that super heap supports "list allocation" (e.g. SegmentHeap)
      auto [start_node, end_node] = Super::malloc(sz, n);
      deq_assert(start_node != nullptr && end_node != nullptr);
      return {start_node, end_node};
    }

    inline void free(node_t* start_node, node_t* end_node) {
      my_deq().push_bottom({start_node, end_node});
    }

    inline size_t getSize(void *ptr) {
      return Super::getSize(ptr);
    }
};

#endif