#include <cstdlib>
#include <iostream>
#include <rapidcheck.h>
#include <thread>
#include "../libdeqalloc.cpp"

template<class... Inputs>
void run_multi_threaded(auto f,
  Inputs... ins,
  size_t nthreads = thread::hardware_concurrency()) {

  std::vector<thread> threads(nthreads);
  for (int i = 0; i < nthreads; i++) threads[i] = thread(f, ins...);
  for (int i = 0; i < nthreads; i++) threads[i].join();
}

int main() {
  rc::check("Single threaded malloc free", [](unsigned short n) {
    RC_PRE(n > 0);
    void* ptr = xxmalloc(n);
    xxfree(ptr);
    RC_ASSERT((uintptr_t)ptr != 0);
  });

  rc::check("Multi threaded malloc free", [](unsigned short n) {
    RC_PRE(n > 0);
    auto f = [&](unsigned short n){
      for(int i = 0; i < n; i++) {
        void* ptr = xxmalloc(n);
        xxfree(ptr);
        RC_ASSERT((uintptr_t)ptr != 0);
      }
    };
    run_multi_threaded<unsigned short>(f, n);
  });
}
