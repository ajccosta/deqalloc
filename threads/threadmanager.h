#ifndef THREADMANAGER_H_
#define THREADMANAGER_H_

#include <atomic>
#include <iostream>

constexpr int max_threads = 1024;

extern inline std::atomic<int>& num_threads() {
  static std::atomic<int> num_threads;
  return num_threads;
}

extern inline int thread_id() {
  static thread_local __attribute__((tls_model("initial-exec"))) int id{num_threads().fetch_add(1)};
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