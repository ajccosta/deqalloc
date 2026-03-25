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
      const size_t thread_count = num_threads();

      // size_t actual_length = 1;
      // auto *node = start_node;
      // while (node != end_node) {
      //   actual_length++;
      //   node = node->next;
      // }

      // heads[tid] is the head of the sublist with owner `tid`
      std::array<node_t *, max_threads> heads;
      heads.fill(nullptr);

      // tails[tid] is the tail of the sublist with owner `tid`
      std::array<node_t *, max_threads> tails;

      // lengths[tid] is the length of the sublist with owner `tid`
      std::array<size_t, max_threads> lengths;

      // Successor of the last node
      auto *succ = end_node->next;

      // Points to node being traversed
      auto *node = start_node;
      while (true) {
        size_t owner_tid = Super::getOwner(node);
        if (heads[owner_tid] == nullptr) {
          // first node with owner
          lengths[owner_tid] = 1;
          heads[owner_tid] = node;
        } else {
          lengths[owner_tid]++;
          tails[owner_tid]->next = node;
        }
        tails[owner_tid] = node;
        if (node == end_node)
          break;
        node = node->next;
      }
      // tid of owner of start node
      size_t start_tid = Super::getOwner(start_node);
      // Now permute nodes in list to group them by owner
      // Running tail of permuted list
      // Have to start with the group owned by the owner of start_node
      // Note that heads[start_tid] == start_node
      auto *tail = tails[start_tid];
      for (size_t tid = 0; tid < thread_count; tid++) {
        // Already have placed list owned by `start_id`
        if (tid == start_tid || heads[tid] == nullptr) continue;
        // Append the list owned by tid
        tail->next = heads[tid];
        tail = tails[tid];
      }
      // Point the successor of the tail of the last sublist to the
      // successor of the end_node
      tail->next = succ;
      for (size_t tid = 0; tid < thread_count; tid++) {
        if (heads[tid] == nullptr)
          continue;
        if (tid == thread_id()) {
          // Owned by this thread
          my_deq().push_bottom({heads[tid], tails[tid], lengths[tid]}, false);
        } else {
          // Owner by another thread
          deques[tid].push_top({heads[tid], tails[tid], lengths[tid]}, false);
        }
      }
      for (size_t tid = 0; tid < thread_count; tid++) {
        if (heads[tid])
          deques[tid].wait();
      }
      // std::abort();
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