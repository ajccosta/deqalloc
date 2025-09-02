#ifndef TRACE_HEAP_H
#define TRACE_HEAP_H

#include <atomic>
#include <cstdio>
#include <fstream>
#include <filesystem>
#include <x86intrin.h>
#include <cstdint>
#include <mutex>
#include "threads/threadmanager.h"

inline uint64_t rdtsc() {
    unsigned hi, lo;
    asm volatile ("rdtsc" : "=a"(lo), "=d"(hi));
    return ((uint64_t)hi << 32) | lo;
}

//Some destructors may be called after a thread exits
//This prevents calling TraceHeap functions after a tracemanager has been destructed
thread_local inline __attribute__((tls_model("initial-exec"))) bool destructed{false};

template <class SuperHeap, class Allocator>
class TraceHeap : public SuperHeap {
  private:

    struct tracemanager {
      private:
        
        enum alloc_t {MALLOC, FREE};

        struct entry_t {
          uint32_t sz;
          uint8_t type;
          uint64_t ts;
        }  __attribute__((packed));

        std::vector<entry_t, STLAllocator<entry_t, Allocator>> entries;

        size_t current = 0;
        static constexpr size_t N = 10000;
        uint64_t last_ts;

        static uint64_t get_ts() {
          //return rdtsc();
          static std::atomic<uint64_t> atomic_ts {0};
          return ++atomic_ts;
        }

        uint64_t get_clock_time() {
          if(++current % N == 0) {
            current = 0;
            last_ts = get_ts();
          }
          return last_ts;
        }

        void add_entry(size_t sz, alloc_t type) {
          uint64_t ts = get_clock_time();
          entry_t e{static_cast<uint32_t>(sz), static_cast<uint8_t>(type), static_cast<uint32_t>(ts)};
          entries.push_back(e);
        }

      public:
        tracemanager() {
          last_ts = get_ts();
        }

        ~tracemanager() {
          static size_t written {0};
          static std::mutex io_mutex;
          {
            std::lock_guard<std::mutex> lk(io_mutex);
            //First thread deletes file, others append
            auto mode = written != 0 ? std::ios::binary | ofstream::app : std::ios::binary;
            written++;
            const std::string &filename = std::filesystem::current_path().string() + "/test.out";
            std::ofstream out(filename, mode);
            if (!out) fprintf(stderr, "Failed to open file for writing\n");
            for(const entry_t& e: entries)
              out.write(reinterpret_cast<const char*>(&e), sizeof(entry_t));
            if (!out) fprintf(stderr, "Failed to write entries to file\n");
            out.close();
            std::cerr << "Thread " << thread_id() << (written == 1 ? " wrote to " : " appended to ") << filename << std::endl;
          }
          destructed = true;
        }

        void malloc(size_t sz) { add_entry(sz, alloc_t::MALLOC); }
        void free(size_t sz) { add_entry(sz, alloc_t::FREE); }
    };

    static inline tracemanager& get_trace_manager() {
      thread_local __attribute__((tls_model("initial-exec"))) tracemanager tm{};
      return tm;
    }

  public:
    TraceHeap() {}
    ~TraceHeap() {}

    inline void* malloc(size_t sz) {
      if(destructed) return nullptr;
      get_trace_manager().malloc(sz);
      return SuperHeap::malloc(sz);
    }

    inline void free(void* ptr, size_t objectSize) {
      if(destructed) return;
      get_trace_manager().free(objectSize);
      SuperHeap::free(ptr, objectSize);
    }

    inline void free(void* ptr) {
      if(destructed) return;
      get_trace_manager().free(SuperHeap::getSize(ptr));
      SuperHeap::free(ptr);
    }
};

#endif