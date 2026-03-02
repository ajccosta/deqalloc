// -*- C++ -*-

/*

  Heap Layers: An Extensible Memory Allocation Infrastructure
  
  Copyright (C) 2000-2020 by Emery Berger
  http://www.emeryberger.com
  emery@cs.umass.edu
  
  Heap Layers is distributed under the terms of the Apache 2.0 license.

  You may obtain a copy of the License at
  http://www.apache.org/licenses/LICENSE-2.0

*/

#ifndef DQ_CONCURRENT_PROFILEHEAP_H
#define DQ_CONCURRENT_PROFILEHEAP_H

#include <atomic>
#include <cstdio>
#include <cstring>
#include "deqalloc/threads/threadmanager.h"

// Maintain & print memory usage info.
// Requires a superheap with the size() method (e.g., SizeHeap).

#define CLASS_NAME __PRETTY_FUNCTION__

#include <string>
#include <iostream>
#include <inttypes.h>

template <int HeapId, class SuperHeap>
class ConcurrentProfileHeap : public SuperHeap {
  private:
    char superheap_type_name[1024];

    const void get_superheap_type(char* type_name) const {
        const char* func = CLASS_NAME;
        const char* start = std::strstr(func, "SuperHeap = ");
        if (!start) return;
        start += std::strlen("SuperHeap = ");
        char* end = (char*) std::strchr(start, '<');
        if (!end) end = (char*) std::strchr(start, ']');
        if (!end) return;
        std::size_t len = std::min(std::size_t(end - start), static_cast<size_t>(1024 - 1));
        std::strncpy(type_name, start, len);
    }  

    //print_stats every <frequency> operations
    static constexpr size_t frequency = 10000;
    size_t _frequency = 0;

    void print_stats(const char* operation) {
      if(_frequency++ == frequency) {
        _frequency = 0;
        fprintf(stderr, "%s[%d]:%s %+" PRId64 "\n",
          superheap_type_name,
          HeapId,
          operation,
          memory_usage.load(std::memory_order_relaxed));


        if(memory_usage.load(std::memory_order_relaxed) >= 184467440736877618ull) deq_assert(false);
      }
    }

    void freq_print_stats(const char* operation) { if(thread_id() == 0) print_stats(operation); }

    std::atomic<int64_t> memory_usage;

  public:
    ConcurrentProfileHeap(): memory_usage(0) { get_superheap_type(superheap_type_name); }
    ~ConcurrentProfileHeap() { print_stats("dtor()"); }

    inline void* malloc(size_t sz) {
      memory_usage.fetch_add(sz);
      freq_print_stats("malloc(sz)");
      return SuperHeap::malloc(sz);
    }

    inline void free(void* ptr, size_t objectSize) {
      memory_usage.fetch_sub(objectSize);
      freq_print_stats("free(ptr, objectSize)");
      SuperHeap::free(ptr, objectSize);
    }

    inline void free(void* ptr) {
      memory_usage.fetch_sub(SuperHeap::getSize(ptr));
      freq_print_stats("free(ptr)");
      SuperHeap::free(ptr);
    }

    //Multi alloc versions
    using node_t = typename SuperHeap::node_t;
    inline std::pair<node_t*, node_t*> malloc(size_t sz, size_t list_length) {
      memory_usage.fetch_add(sz*list_length);
      freq_print_stats("multi_malloc(sz, list_length)");
      return SuperHeap::malloc(sz, list_length);
    }

    inline void free(node_t* start, node_t* end) {
      SuperHeap::free(start, end);
      return; //TODO change this when threadlocalstack is working correctly

      size_t num_nodes = 1;
      node_t* next = start;
      while(next != end) { next = next->next; num_nodes++; }
      memory_usage.fetch_sub(SuperHeap::getSize(start)*num_nodes);
      freq_print_stats("multi_free(start, end)");
    }
};

#endif
