/* -*- C++ -*- */

/*

  Heap Layers: An Extensible Memory Allocation Infrastructure
  
  Copyright (C) 2000-2017 by Emery Berger
  http://www.emeryberger.com
  emery@cs.umass.edu
  
  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; either version 2 of the License, or
  (at your option) any later version.
  
  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.
  
  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/


#include <stdlib.h>

volatile int anyThreadCreated = 1;

#include "heaplayers.h"

#define NULL_FREE //Whether to deal with free(nullptr);

using namespace HL;

#define SEGMENT_SIZE 2*1024*1024 //2 MiB
#define SMALL_SIZE_CLASS_MAX 32*1024 //32 KiB

class TheDeqallocHeapType : public MiniSegHeap<
                                     SMALL_SIZE_CLASS_MAX,
                                     ThreadLocalStack<
                                       DequeHeap<
                                         SegmentHeap<SEGMENT_SIZE, SMALL_SIZE_CLASS_MAX>>>,
                                     SegmentHeap<SEGMENT_SIZE, SMALL_SIZE_CLASS_MAX>> {};

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
