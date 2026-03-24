#ifndef DEQUE_HEAP_H
#define DEQUE_HEAP_H

#include <optional>
#include <atomic>

#include "deqalloc/heaps/top/segmentheap.h"
#include "deqalloc/threads/structures/memorystealingdeque.h"
#include "deqalloc/threads/structures/fcdeque.h"
#include "deqalloc/threads/threadmanager.h"
#include "deqalloc/utility/random.h"

template <class Super = SegmentHeap<>>
class DequeHeap : public Super {
  public:

    enum { Alignment = Super::Alignment };

    using node_t = typename Super::node_t;

  private:

#ifndef FC_DEQUE
    using deque_t = MemoryStealingDeque<std::tuple<node_t*, node_t*>, 2, 1>;
#elif defined(REMOTE_FREE)
    using deque_t = FCDeque<std::tuple<node_t*, node_t*, size_t>>;
#else
    using deque_t = FCDeque<std::tuple<node_t*, node_t*>>;
#endif
    deque_t deques[max_threads];
    static inline thread_local parlay::random my_rand __attribute__((tls_model ("initial-exec")));

    inline deque_t& my_deq() { return deques[thread_id()]; }
    inline deque_t& random_deq(size_t n_threads) { return deques[my_rand.rand() % n_threads]; }

  public:

    inline auto malloc(size_t sz, size_t n) {
      auto opt = my_deq().pop_bottom();
      //Try our own deque
      if (opt) {
#ifndef REMOTE_FREE
        auto [start_node, end_node] = opt.value();
        deq_assert(start_node != nullptr && end_node != nullptr);
        return std::make_pair(start_node, end_node);
#else
        auto [start_node, end_node, len] = opt.value();
        deq_assert(start_node != nullptr && end_node != nullptr);
        return std::make_tuple(start_node, end_node, len);
#endif
      }
      //Our deque is empty, try stealing
      //Avoid atomic read of num_threads multiple times
      size_t n_threads = num_threads().load(std::memory_order_relaxed);
      size_t attempts = 0;
      while(attempts++ < n_threads) {
        //TODO re-attempt if deque is not empty?
        auto res = random_deq(n_threads).pop_top();
        auto [opt, deque_is_empty] = res;
        if (opt) {
#ifndef REMOTE_FREE
        auto [start_node, end_node] = opt.value();
        deq_assert(start_node != nullptr && end_node != nullptr);
        return std::make_pair(start_node, end_node);
#else
        auto [start_node, end_node, len] = opt.value();
        deq_assert(start_node != nullptr && end_node != nullptr);
        return std::make_tuple(start_node, end_node, len);
#endif
        }
      }
      //Could not steal either, allocate from super heap
      //  Currently, we assume that super heap supports "list allocation" (e.g. SegmentHeap)
      auto [start_node, end_node] = Super::malloc(sz, n);
      deq_assert(start_node != nullptr && end_node != nullptr);
#ifdef REMOTE_FREE
      return std::make_tuple(start_node, end_node, n);
#else
      return std::make_pair(start_node, end_node);
#endif
    }

    inline void free(node_t* start_node, node_t* end_node) {
      deq_assert(start_node != nullptr && end_node != nullptr);
#ifdef REMOTE_FREE
      auto *curr_start = start_node;
      auto *curr_end = start_node;
      size_t len = 1;
      while (true) {
        auto *next = curr_end->next;
        size_t owner_tid = Super::getOwner(curr_end);
        if (curr_end == end_node || owner_tid != Super::getOwner(next)) {
          // end of contiguous region
          if (owner_tid == thread_id()) {
            // push to local deque
            my_deq().push_bottom({curr_start, curr_end, len});
          } else {
            // push to remote deque
            deques[owner_tid].push_top({curr_start, curr_end, len});
          }
          if (curr_end == end_node)
            // processed all nodes
            break;
          curr_start = next;
          len = 1;
        } else {
          len++;
        }
        curr_end = next;
      }
#else
      my_deq().push_bottom({start_node, end_node});
#endif
    }

    inline size_t getSize(void *ptr) {
      return Super::getSize(ptr);
    }

    static constexpr inline size_t SegmentNumNodes(size_t sz) {
      return Super::SegmentNumNodes(sz);
    }
};

#endif