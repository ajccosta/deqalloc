#ifndef DEQUE_HEAP_H
#define DEQUE_HEAP_H

#include <optional>
#include <atomic>
// #include <bitset>

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
#else
    using deque_t = FCDeque<std::tuple<node_t*, node_t*>>;
#endif
    deque_t deques[max_threads];
    static inline thread_local parlay::random my_rand __attribute__((tls_model ("initial-exec")));

    inline deque_t& my_deq() { return deques[thread_id()]; }
    inline deque_t& random_deq(size_t n_threads) { return deques[my_rand.rand() % n_threads]; }

    struct FreedSegment {
      node_t *head = nullptr;
      node_t *tail = nullptr;
      size_t len = 0;
    };

    std::array<std::array<FreedSegment, max_threads>, max_threads> remote_free_lists;

  public:

    inline auto malloc(size_t sz, size_t n) {
      auto opt = my_deq().pop_bottom();
      //Try our own deque
      if (opt) {
        auto [start_node, end_node] = opt.value();
        deq_assert(start_node != nullptr && end_node != nullptr);
        return std::make_pair(start_node, end_node);
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
          auto [start_node, end_node] = opt.value();
          deq_assert(start_node != nullptr && end_node != nullptr);
          return std::make_pair(start_node, end_node);
        }
      }
      //Could not steal either, allocate from super heap
      //  Currently, we assume that super heap supports "list allocation" (e.g. SegmentHeap)
      auto [start_node, end_node] = Super::malloc(sz, n);
      deq_assert(start_node != nullptr && end_node != nullptr);
      return std::make_pair(start_node, end_node);
    }

#ifdef REMOTE_FREE

    inline void free(node_t *start_node, node_t *end_node, size_t list_length) {
      deq_assert(start_node != nullptr && end_node != nullptr);
      const size_t my_tid = thread_id();
      auto &rfl = remote_free_lists[my_tid];
      // std::bitset<max_threads> waiting;

      // Points to node being traversed
      auto *node = start_node;
      while (true) {
        size_t owner_tid = Super::getOwner(node);
        auto &seg = rfl[owner_tid];
        if (seg.head == nullptr) {
          // first node with owner
          seg.len = 1;
          seg.head = node;
        } else {
          seg.len++;
          seg.tail->next = node;
        }
        seg.tail = node;
        auto *next = node->next;
        if (seg.len == list_length) {
          node->next = nullptr;
          if (owner_tid == thread_id()) {
            my_deq().push_bottom_direct({seg.head, seg.tail});
          } else {
            deques[owner_tid].push_top_direct({seg.head, seg.tail});
          }
          seg.head = nullptr;
          // waiting.set(owner_tid);
        }
        if (node == end_node)
          break;
        node = next;
      }
      // for (size_t tid = 0, nthreads = num_threads(); tid < nthreads; tid++) {
      //   if (waiting[tid])
      //     deques[tid].wait();
      // }
    }

#else

    inline void free(node_t* start_node, node_t* end_node) {
      deq_assert(start_node != nullptr && end_node != nullptr);
      my_deq().push_bottom({start_node, end_node});
    }

#endif

    inline size_t getSize(void *ptr) {
      return Super::getSize(ptr);
    }

    static constexpr inline size_t SegmentNumNodes(size_t sz) {
      return Super::SegmentNumNodes(sz);
    }
};

#endif