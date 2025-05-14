// ***************************
// micro-epoch  (Author: Guy Blelloch; modified by: André Costa)
// epoch based delayed reclamation 
// the interface consists of three functions
//   template <typename F>
//   uepoch::with_epoch(const F& f) : takes a lambda or function f
//      with no arguments and with or without a result, and runs it in
//      protected mode; and takes in a lambda or function advance, that
//      called in the case that an epoch advancement is detected in the
//      current thread (allows for custom epoch advancement).
// with_epoch can be safely nested (all but the outermost is ignored)
//
// This implementation is tailored to SegmentHeap's needs. We use a
//   custom advance function because we want to try to avoid performing
//   any allocations. This way, SegmentHeap can use fields in its header
//   to keep track of the epoch bags.
//
// Example:
//     auto r = uepoch::with_epoch(
//        [&] { ... x = y->left; ... return z;},
//        [&] { free(safe); old=current; current.empty(); }
//     );
// ***************************

#ifndef UEPOCH_H_
#define UEPOCH_H_

#include <atomic>
#include "threadmanager.h"

namespace uepoch {
  namespace internal {

    // the main structure
    struct alignas(64) epoch_state {

      // announcements for each thread
      struct alignas(64) announce {
        std::atomic<long> last;
        announce() : last(-1l) {}};

      // the various delayed lists for each thread
      struct alignas(256) thread_state {
        long epoch = 0; // epoch on last delay
        long count = 0; // number of calls to delay since last epoch update
        thread_state() {}
      };

      // the full state
      std::atomic<long> current_epoch{0};
      announce announcements[max_threads];
      thread_state pools[max_threads];
  
      epoch_state() {}
      ~epoch_state() {}

      // adds current epoch to the announcement slot of this thread
      std::pair<bool,int> announce() {
        size_t id = thread_id();
        if (announcements[id].last.load() == -1l) {
          announcements[id].last = current_epoch.load();
          return std::pair(true, id);
        } else return std::pair(false, id);
      }

      void unannounce(size_t id) {
        announcements[id].last.store(-1l, std::memory_order_relaxed); }

      // run every once in a while to check if epoch can be incremented
      void update_epoch() {
        long current_e = current_epoch.load();
        int threads;
        while (true) { // unlikely to loop more than once
          threads = num_threads();
          for (int i=0; i < threads; i++)
            if ((announcements[i].last != -1l) &&
                announcements[i].last < current_e) return;
          if (num_threads() == threads) break;
        }
        current_epoch.compare_exchange_strong(current_e, current_e+1);
      }

      template <typename AdvanceThunk>
      void advance_epoch(int i, const AdvanceThunk& advance) {
        thread_state& pid = pools[i];
        auto curr_ep = current_epoch.load();
        if (pid.epoch + 1 < curr_ep) {
          advance();
          pid.epoch = curr_ep;
        }
        long heuristic_threshold = 2 * num_threads();
        if (++pid.count == heuristic_threshold) { 
          pid.count = 0;
          update_epoch();
        }
      }
    }; // end struct epoch_state
  
    extern inline epoch_state& get_epoch() {
      static epoch_state epoch;
      return epoch;
    }
  } // end namespace internal

  //f: what to execute inside an epoch
  //advance: what to execute in case of an epoch advancement
  template <typename Thunk, typename AdvanceThunk>
  auto with_epoch(const Thunk& f, const AdvanceThunk& advance,
    uepoch::internal::epoch_state& epoch = internal::get_epoch()) {

    auto [not_in_epoch, id] = epoch.announce();
    if constexpr (std::is_void_v<std::invoke_result_t<Thunk>>) {
      f();
      epoch.advance_epoch(id, advance);
      if (not_in_epoch) epoch.unannounce(id);
    } else {
      auto v = f();
      epoch.advance_epoch(id, advance);
      if (not_in_epoch) epoch.unannounce(id);
      return v;
    }
  }

  template <typename Thunk>
  auto with_epoch(const Thunk& f,
    uepoch::internal::epoch_state& epoch = internal::get_epoch()) {
    //Typical with_epoch, i.e., does nothing on epoch advancement.
    //Hopefully the empty lambda is optimized out.
    return with_epoch(f, [](){}, epoch);
  }

  template <typename AdvanceThunk>
  auto quiescence_check(const AdvanceThunk& advance,
    uepoch::internal::epoch_state& epoch = internal::get_epoch()) {
    //Don't do anything besides react to an epoch advancement
    //Hopefully the empty lambda is optimized out.
    return with_epoch([](){}, advance, epoch);
  }
} // end namespace epoch

#endif //PARLAY_EPOCH_H_