#ifndef THREADMANAGER_H_
#define THREADMANAGER_H_

#include <atomic>
#include <iostream>

constexpr size_t max_threads = 256;

extern inline std::atomic<size_t>& num_threads() {
  static std::atomic<size_t> num_threads;
  return num_threads;
}

extern inline size_t thread_id() {
  static thread_local __attribute__((tls_model("initial-exec"))) size_t id{num_threads().fetch_add(1)};
#ifndef NDEBUG
  if (id > max_threads) {
    std::cerr << "Too many threads. To fix this, increase the maximum "
    << "number of threads (currently " << max_threads << ")." << std::endl;
    abort();
  }
#endif
  return id;
}

#endif
