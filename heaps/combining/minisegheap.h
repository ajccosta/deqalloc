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

#ifndef HL_MINISEGHEAP_H
#define HL_MINISEGHEAP_H

/**
 * @class MiniSegHeap
 * @brief A minimalist segheap.
 * Does not allocate from big heap when a small heap allocation fails.
 * Does not rely on segheap.
 * Does not take size2Class/class2Size functions, it uses mimalloc's
 *
 * @param SmallHeap The subheap class.
 * @param LargeHeap The parent class, used for "big" objects.
 */

template <size_t NumBins, class SmallHeap, class LargeHeap>
class MiniSegHeap : public SmallHeap {
  private:
    SmallHeap smallHeaps[NumBins];
    LargeHeap largeHeap;

    static constexpr size_t smallestSize = 64;
    static constexpr size_t skippedClasses = HL::ilog2(smallestSize);

    static inline constexpr size_t class2Size(const size_t i) {
      return (size_t) (1ULL << (i+skippedClasses));
    }

    static constexpr size_t maxSmallObjectSize = class2Size(NumBins-1);

    static inline constexpr int size2Class(const size_t sz) {
      return (int) HL::ilog2((sz < smallestSize) ? smallestSize : sz) - skippedClasses;
    }

  public:

    enum { Alignment = gcd<LargeHeap::Alignment, SmallHeap::Alignment>::VALUE };

    inline void* malloc(const size_t sz) {
      void* ptr = nullptr;
      const auto sizeClass = size2Class(sz);
      const auto realSize = class2Size(sizeClass);
      deq_assert(realSize >= sz);
      if (realSize <= maxSmallObjectSize) {
        deq_assert(sizeClass >= 0);
        deq_assert(sizeClass < NumBins);
        ptr = smallHeaps[sizeClass].malloc(realSize);
      } else {
        ptr = largeHeap.malloc(realSize);
      }
      return ptr;
    }

    inline void free(void* ptr) {
      const auto objectSize = getSize(ptr);
      free(ptr, objectSize);
    }

    inline void free(void* ptr, size_t objectSize) {
      if (objectSize > maxSmallObjectSize) {
        largeHeap.free(ptr);
      } else {
        auto objectSizeClass = size2Class(objectSize);
        deq_assert (objectSizeClass >= 0);
        deq_assert (objectSizeClass < NumBins);
        smallHeaps[objectSizeClass].free(ptr, objectSize);
      }
    }

    inline size_t getSize(void* ptr) {
      return SmallHeap::getSize(ptr);
    }
};

#endif