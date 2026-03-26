#ifndef THREADMANAGER_H_
#define THREADMANAGER_H_

#include <atomic>
#include <iostream>
#include <array>

#ifdef FC_DEQUE
constexpr size_t max_threads = 256;
#else
constexpr size_t max_threads = 1024;
#endif

static std::atomic<size_t> thread_counter{0};

//default initialized to false, so we use false as "available"
static std::array<std::atomic<bool>, max_threads> available_ids;
constexpr static bool available = false;
constexpr static bool unavailable = !available;



struct threadmanager {
    size_t id;

    threadmanager() {
        id = 0;
        bool t = available;
        for(; id < max_threads; id++) {
            if(available_ids[id].load(std::memory_order_seq_cst) == available) {
                if(available_ids[id].compare_exchange_strong(t, unavailable)) {
                    break;
                }
                t = available; //read value (unavailable) was loaded into t
            }
        }
        if (id >= max_threads) { //No id available
            std::cerr << "deqalloc: Too many threads. To fix this, increase the maximum "
                      << "number of threads (currently " << max_threads << ") in "
                      << __FILE__ << ":" << __LINE__ << std::endl;
            std::abort();
        }
        thread_counter.fetch_add(1);
    }

    ~threadmanager() {
        deq_assert(available_ids[id].load() == unavailable);
        available_ids[id].store(available, std::memory_order_seq_cst);
        thread_counter.fetch_sub(1);
    }
};

extern inline std::atomic<size_t>& num_threads() {
  return thread_counter;
}

static inline __attribute__((always_inline)) size_t thread_id() noexcept {
    static thread_local __attribute__((tls_model ("initial-exec"))) threadmanager manager;
    return manager.id;
}

#endif
