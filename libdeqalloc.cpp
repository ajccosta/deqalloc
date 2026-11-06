/* -*- C++ -*- */

/*

  Heap Layers: An Extensible Memory Allocation Infrastructure
  
  Copyright (C) 2000-2020 by Emery Berger
  http://www.emeryberger.com
  emery@cs.umass.edu
  
  Heap Layers is distributed under the terms of the Apache 2.0 license.

  You may obtain a copy of the License at
  http://www.apache.org/licenses/LICENSE-2.0

*/


#include <stdlib.h>

volatile int anyThreadCreated = 1;


#define SEGMENT_SIZE 2*1024*1024 //2 MiB
#define SMALL_SIZE_CLASS_MAX 32*1024 //32 KiB
#ifndef DEFAULT_LIST_BYTES
  #define DEFAULT_LIST_BYTES 16*1024ull //16KiB
#endif

#include "tprintf.h"
#include "heaplayers.h"
#include "deqalloc.h"

#include "deqalloc/utility/assert.h"

#define NULL_FREE //Whether to deal with free(nullptr);

//#define TRACE

using namespace HL;

class TheDeqallocHeapType_ : public MiniSegHeap<
                                     SMALL_SIZE_CLASS_MAX,
                                     ThreadLocalStack<
                                       DequeHeap<
                                         SegmentHeap<SEGMENT_SIZE, SMALL_SIZE_CLASS_MAX>>>,
                                     SegmentHeap<SEGMENT_SIZE, SMALL_SIZE_CLASS_MAX>> {};

#ifdef TRACE
  using TheDeqallocHeapType = TraceHeap<TheDeqallocHeapType_, SegmentHeap<SEGMENT_SIZE, SMALL_SIZE_CLASS_MAX>>;
#else
  using TheDeqallocHeapType = TheDeqallocHeapType_;
#endif

inline static TheDeqallocHeapType* getDeqallocHeap() {
  static char thBuf[sizeof(TheDeqallocHeapType)];
  static TheDeqallocHeapType * th = new (thBuf) TheDeqallocHeapType;
  return th;
}

#if defined(_WIN32)
#pragma warning(disable:4273)
#endif

#include "printf.h"

#if !defined(_WIN32)
#include <unistd.h>

extern "C" {
  // For use by the replacement printf routines (see
  // https://github.com/emeryberger/printf)
  void _putchar(char ch) { auto s = ::write(1, (void *)&ch, 1); }
}
#endif

#include "wrappers/generic-memalign.cpp"

extern "C" {
  
  void * xxmalloc (size_t sz) {
    auto ptr = getDeqallocHeap()->malloc(sz);
    deq_assert((uintptr_t)ptr % 8 == 0); //Should atleast be 8 bytes aligned
    return ptr;
  }

  void xxfree (void * ptr) {
#ifdef NULL_FREE
    if(ptr == nullptr) return;
#endif
    getDeqallocHeap()->free(ptr);
  }

  void xxfreesz (void * ptr, size_t sz) {
#ifdef NULL_FREE
    if(ptr == nullptr) return;
#endif
    getDeqallocHeap()->free(ptr, sz);
  }

  void * xxmemalign(size_t alignment, size_t sz) {
    auto ptr = generic_xxmemalign(alignment, sz);
    deq_assert((uintptr_t)ptr % alignment == 0);
    return ptr;
  }
  
  size_t xxmalloc_usable_size (void * ptr) {
    return getDeqallocHeap()->getSize (ptr);
  }

  void xxmalloc_lock() {
    fprintf(stderr, "Cannot lock heap!");
    exit(-1);
  }

  void xxmalloc_unlock() {
    fprintf(stderr, "Cannot lock heap!");
    exit(-1);
  }
  
}
