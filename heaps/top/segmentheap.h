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

#ifndef HL_SEGMENTHEAP_H
#define HL_SEGMENTHEAP_H

#include "heaps/top/mmapheap.h"
#include "threads/cpuinfo.h"

/**
 * @class SegmentHeap
 * @brief Heap that always allocates segments of the same size
 * @author André Costa
 */

#define DEFAULT_SEGMENT_SIZE 4 * 1024 * 1024 //4MiB

#include <iostream>

namespace HL {

  template<size_t SegmentSize_ = DEFAULT_SEGMENT_SIZE>
  class SegmentHeap : public SizedMmapHeap {
    public:
      static const size_t SegmentSize = SegmentSize_; //Make SegmentSize visible to other classes
      enum { Alignment = SegmentSize };

    private:

      // Note: we never reclaim memory obtained for MyHeap, even when
      // this heap is destroyed.
      class MyHeap : public LockedHeap<PosixLockType, FreelistHeap<BumpAlloc<65536, SizedMmapHeap>>> {};
      typedef std::unordered_map<void*, void*,
          std::hash<void*>, std::equal_to<void*>,
          HL::STLAllocator<std::pair<void* const, void* const>, MyHeap>>
        mapType;

      mapType MyMap;
      PosixLockType MyMapLock;

      //Register newly created segment in map
      //Base: pointer to first byte in segment; sz: size of segment
      void registerSegment(void* base) {
        void* alignedBase = getAlignedBase(base);
        MyMapLock.lock();
        MyMap[alignedBase] = base;
        MyMapLock.unlock();
      }

      void* getBasePointer(void* ptr) {
        void* alignedBase_1 = getAlignedBase(ptr);
        void* alignedBaseEnd_1 = getSegmentEnd(alignedBase_1);
        void* alignedBase_2 = getAlignedBase(alignedBaseEnd_1 + 1);
        void* alignedBaseEnd_2 = getSegmentEnd(alignedBase_1);
        assert(alignedBase_1 + SegmentSize == alignedBase_2);
        MyMapLock.lock();
        auto search = MyMap.find(alignedBase_1);
        if (search != MyMap.end() && 
          ptr >= alignedBase_1 && ptr <= alignedBaseEnd_1) {}
        else {
          search = MyMap.find(alignedBase_2);
          assert(search != MyMap.end());
        }
        MyMapLock.unlock();
        const auto& value = search->second;
        return value;
      }

      void* allocateSegment() {
        void* ptr = SizedMmapHeap::malloc(SegmentSize);
        registerSegment(ptr);
        return const_cast<void*>(ptr);
      }

      void freeSegment(void* ptr) {
        SizedMmapHeap::free(ptr, SegmentSize);
      }

      //Returns the last address of segment whose start is base
      static inline void* getSegmentEnd(void* base) {
        return base + SegmentSize - 1;
      }

      //Returns the pointer to the SegmentSize aligned region which base belongs to
      static inline void* getAlignedBase(void* base) {
        void* ret = (void*)((uintptr_t) base & (~(SegmentSize-1)));
        assert(reinterpret_cast<size_t>(ret) % SegmentSize == 0);
        return ret;
      }

    public:

      //SegmentSize_ must be multiple of page size
      static_assert(SegmentSize % CPUInfo::PageSize == 0);

      inline void* malloc([[maybe_unused]] size_t sz) {
        void* ptr = allocateSegment();
        return const_cast<void*>(ptr);
      }

      inline void free(void* ptr) {
        void* base = getBasePointer(ptr);
        freeSegment(base);
      }
  };

}

#endif