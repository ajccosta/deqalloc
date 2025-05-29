// ***************************
// micro-fetch-and-add-epoch  (Author: André Costa, based on micro-epoch by Guy Blelloch)
// Specialization of an epoch-based collector where there only a single atomic integer is
// kept to keep track on the number of in-flight threads. This is useful in cases where a
// thread has distinguished access to the structure protected by the collector (e.g., a 
// work-stealing deque). In that case, we want to avoid forcing the the distinguised thread
// from having to scan every other thread's epoch announcement, but it's fine if the rarer
// non-distinguished accesses from other threads have to perform extra/heavier work.
// Therefore, the distinguished thread simply checks if the counter is 0, while other threads
// must increment and decrement the counter (with a fetch-and-add op., hence the name).
//
// The distinguised thread calls quiescence_check, other threads call with_epoch.
// ***************************

#ifndef FAA_UEPOCH_H_
#define FAA_UEPOCH_H_

#include <atomic>

namespace faa_uepoch {
  struct alignas(64) faa_epoch_state {
    private:
      //number of in-flight threads
      std::atomic_uint_fast64_t inflight{0};

    public:
      faa_epoch_state() {}
      ~faa_epoch_state() {}

      //non-distinguished threads
      inline void announce() { inflight.fetch_add(1, std::memory_order_seq_cst); }
      inline void unannounce() { inflight.fetch_sub(1, std::memory_order_seq_cst); }

      //distinguished thread
      inline bool check_inflight() { return inflight.load(std::memory_order_relaxed) == 0; };

      //f: what to execute inside an epoch
      template <typename Thunk>
      auto with_epoch(const Thunk& f) {
        announce();
        if constexpr (std::is_void_v<std::invoke_result_t<Thunk>>) {
          f();
          unannounce();
        } else {
          auto v = f();
          unannounce();
          return v;
        }
      }

      template <typename AdvanceThunk>
      auto quiescence_check(const AdvanceThunk& advance) {
        //Don't do anything besides react to a positive quiescence check
        if(check_inflight())
          advance();
      }
  }; // end struct faa_epoch_state
} // end namespace faa_epoch

#endif