#ifndef MEMORYSTEALINGDEQUE_H_
#define MEMORYSTEALINGDEQUE_H_

//Continuous array version of ABP work-stealing deque modified so that
//  the first N elements (from the owner's perspective) cannot be stolen
//
//  Author: André Costa

#include <cassert>

#include <atomic>
#include <utility>
#include <optional>

#include "continuousarray.h"
#include "../epoch.h"

// Deque based on "Correct and Efficient Work-Stealing for Weak Memory Models" from
//  Nhat Minh Lê, Antoniu Pop, Albert Cohen and Francesco Zappa Nardelli, which itself
//  is based on the Deque in "Dynamic Circular Work-Stealing Deque" from David Chase
//  and Yossi Lev which was based on Arora, Blumofe, and Plaxton SPAA, 1998.
//  A nice chain of work :-).
//
// Supports:
//
// push_bottom:  Only the owning thread may call this
// pop_bottom:   Only the owning thread may call this
// pop_top:      Non-owning threads may call this
//
template <typename V,
  size_t N, //At which num_pops value should we attempt to "drop the anchor"
  size_t anchor_drop> //At which num_pops value should we attempt to "drop the anchor"
struct alignas(64) MemoryStealingDeque {

  size_t last_anchor; //Where was last anchor dropped
  int64_t num_pops; //#pop ops that the owner is allowed to do without syncing
  alignas(64) continuous_array<V> deq;
  alignas(64) uepoch::internal::epoch_state epoch; //TODO implement specialized epoch collector for deque pattern
  alignas(64) std::atomic<uint64_t> bot; //index of where owner is pushing/popping
  alignas(64) std::atomic<uint64_t> top; //index of where thiefs are stealing

  MemoryStealingDeque() :
    last_anchor(0), num_pops(0), bot(0), top(0) {}

  ~MemoryStealingDeque() {}

  // Adds a new val to the bottom of the queue. Only the owning
  // thread can push new items. This must not be called by any
  // other thread.
  bool push_bottom(V val) {
    auto b = bot.load(std::memory_order_relaxed); // atomic load
    deq.put_head(b, val);
    bot.store(b + 1, std::memory_order_seq_cst);  // shared store
    if (num_pops < (int64_t) N) { //Don't surpass maximum number of num_pops
        num_pops++; //We can do 1 more syncless pop because of this push
    }
    last_anchor = b + 1;
    return true; //We use this to count every successful push op. (which is all of them)
  }

  // Pop an item from the top of the queue, i.e., the end that is not
  // pushed onto. Threads other than the owner can use this function.
  //
  // Returns {val, empty}, where empty is true if val was the
  // only val on the queue, i.e., the queue is now empty
  std::pair<std::optional<V>, bool> pop_top() {
    auto t = top.load(std::memory_order_acquire);    // atomic load
    std::atomic_thread_fence(std::memory_order_seq_cst);
    auto b = bot.load(std::memory_order_acquire);  // atomic load
    assert(b + 1 >= t); //Check invariant that bot never strays more than 1 from top
    if (b > t + N) {
      return uepoch::with_epoch([&]() -> std::pair<std::optional<V>, bool> {
        if (top.compare_exchange_strong(t, t + 1)) {
          auto val = deq.get_tail(t);
          return {val, (b == t + 1 + N)};
        } else {
          return {std::nullopt, (b == t + 1 + N)};
        }
      }, epoch);
    }
    return {std::nullopt, true};
  }

  // Pop an item from the bottom of the queue. Only the owning
  // thread can pop from this end. This must not be called by any
  // other thread.
  std::optional<V> pop_bottom() {
    uint64_t t;
    auto b = bot.load(std::memory_order_relaxed); // atomic load
    if (b == 0) {
      return std::nullopt;
    }
    b--;
    assert(b != UINT64_MAX); //Check for underflow
    bot.store(b, std::memory_order_relaxed); // shared store
    if (num_pops > 0) { //Safe to pop without synchronizing
      num_pops--;
      //No extra checks needed. bot was guaranteed to be > 0
      if (num_pops <= anchor_drop) {
        uepoch::quiescence_check([&] {
          std::atomic_thread_fence(std::memory_order_seq_cst);
          if(last_anchor != 0 && b < last_anchor) {
            //If b is greater than last_anchor the public portion does not grow downward
            auto old_num_pops = num_pops;
            t = top.load(std::memory_order_relaxed);
            //We can do more pops than before but don't allow invalid state
            num_pops = std::min(N - (last_anchor - b), b - t); 
            assert(b - num_pops >= t); //Check invariant that bot never strays more than 1 from top
          }
          last_anchor = b; //drop anchor at current bot value
        }, epoch);
      }
      return deq.get_head(b);
    }
    //TODO if we read top here, dont read again
    // Don't uncomment, the following code currently leads to some concurrency bug
    //if (qm->quiescence_check()) {
    //  std::atomic_thread_fence(std::memory_order_seq_cst);
    //  if(last_anchor != 0 && b < last_anchor) {
    //    //If b is greater than last_anchor the public portion does not grow downward
    //    auto old_num_pops = num_pops;
    //    auto t = top.load(std::memory_order_relaxed);
    //    //We can do more pops than before but don't allow invalid state
    //    num_pops = MIN(N - (last_anchor - b), b - t); 
    //  }
    //  last_anchor = b; //we performed a barrier and quiescence check, drop anchor
    //  if (num_pops > 0) { //try again to perform syncless pop
    //    num_pops--;
    //    return deq->get_head(b);
    //  }
    //}
    std::atomic_thread_fence(std::memory_order_seq_cst);
    t = top.load(std::memory_order_relaxed); // atomic load
    std::optional<V> val;
    if (t <= b) {
      val = deq.get_head(b);
      if (t == b) {
        if(!top.compare_exchange_strong(t, t + 1, std::memory_order_seq_cst, std::memory_order_relaxed)) {
          val = std::nullopt;
        }
        bot.store(b + 1, std::memory_order_relaxed);
      }
    } else {
      val = std::nullopt;
      bot.store(b + 1, std::memory_order_relaxed);
    }
    return val;
  }

  //Size from a thief's perspective
  size_t thief_size() {
    return real_size() - N;
  }

  size_t real_size() {
    //Probably don't need this fence but this should only
    //  be called at shutdown so might as well be safe.
    std::atomic_thread_fence(std::memory_order_seq_cst);
    return bot.load(std::memory_order_seq_cst) - top.load(std::memory_order_seq_cst);
  }

  //Can (probably?) be called concurrently, but probably shouldn't!
  //If called concurrently with other threads pushing, there is not guarantee
  //  that the Deque will be completely empty by the time clear() finishes.
  void clear() {
    while(real_size() != 0) { pop_bottom(); }
  }
};

#endif