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
 * Does not take size2Class/class2Size functions, it uses its own (derived from jemalloc and hoard).
 *
 * @param SmallHeap The subheap class.
 * @param LargeHeap The parent class, used for "big" objects.
 */

//maxSmallObjectSize: largest smallest object size
template <size_t maxSmallObjectSize, class SmallHeap, class LargeHeap>
class MiniSegHeap : public SmallHeap {
  private:
  
    //jemalloc's size classes
    static constexpr size_t size_classes[] = {
        // Tiny to small (8 B to 4 KiB)
        8, 16, 32, 48, 64, 80, 96, 112, 128,
        160, 192, 224, 256, 320, 384, 448, 512,
        640, 768, 896, 1024,       // 1 KiB
        1280, 1536, 1792, 2048,    // 2 KiB
        2560, 3072, 3584, 4096,    // 4 KiB
        // Mid-size (5 KiB to 64 KiB)
        5 * 1024, 6 * 1024, 7 * 1024, 8 * 1024,
        10 * 1024, 12 * 1024, 14 * 1024, 16 * 1024,
        20 * 1024, 24 * 1024, 28 * 1024, 32 * 1024,
        40 * 1024, 48 * 1024, 56 * 1024, 64 * 1024,
        // Large (80 KiB to 1 MiB)
        80 * 1024, 96 * 1024, 112 * 1024, 128 * 1024,
        160 * 1024, 192 * 1024, 224 * 1024, 256 * 1024,
        320 * 1024, 384 * 1024, 448 * 1024, 512 * 1024,
        640 * 1024, 768 * 1024, 896 * 1024, 1024 * 1024, // 1 MiB
        // Huge (1280 KiB and up)
        1280 * 1024, 1536 * 1024, 1792 * 1024,
        2 * 1024 * 1024, 2560 * 1024, 3 * 1024 * 1024,
        3584 * 1024, 4 * 1024 * 1024, 5 * 1024 * 1024,
        6 * 1024 * 1024, 7 * 1024 * 1024, 8 * 1024 * 1024,
        10 * 1024 * 1024, 12 * 1024 * 1024, 14 * 1024 * 1024,
        16 * 1024 * 1024, 20 * 1024 * 1024, 24 * 1024 * 1024,
        28 * 1024 * 1024, 32 * 1024 * 1024, 40 * 1024 * 1024,
        48 * 1024 * 1024, 56 * 1024 * 1024, 64 * 1024 * 1024
    };
    
    //statically find the size class corresponding to a given size
    static constexpr int findSizeClassIndex(size_t size) {
      for (size_t i = 0; i < std::size(size_classes); i++)
        if (size_classes[i] == size)
          return i;
      return -1; //Not found, this should error at compile time
    }

    //get the smallest type that will hold a given value
    template <std::size_t N>
    using smallest_unsigned_t =
      std::conditional_t<N <= UINT8_MAX,  uint8_t,
      std::conditional_t<N <= UINT16_MAX, uint16_t,
      std::conditional_t<N <= UINT32_MAX, uint32_t,
                                        uint64_t>>>;

    //The class type. We care about this being the smalles possible because
    // we precompute small size class sizes, and want to save space.
    using class_t = smallest_unsigned_t<std::size(size_classes)>;

    static inline constexpr size_t class2Size(const class_t cl) {
      return size_classes[cl];
    }

    static constexpr int _NumBins = findSizeClassIndex(maxSmallObjectSize) + 1;
    static_assert(_NumBins != -1); //No size class of size maxSmallObjectSize
    static constexpr class_t NumBins = static_cast<class_t>(_NumBins);
    //largest smallest object class (redundant but useful for assertions)
    static constexpr class_t maxSmallObjectClass = findSizeClassIndex(maxSmallObjectSize);
    static_assert(maxSmallObjectClass == NumBins-1);

    static inline class_t size2Class(const size_t sz) {
      static class_t sizes[maxSmallObjectSize+1];
      static bool init = size2Class_precompute(sizes); //static bool so that this is only called once
      return sizes[sz];
    }

    static inline class_t size2Class_slow(const size_t sz) {
      return size2Class_search(sz);
    }

    //Use hoard's size class binary search to precompute the size classes
    //https://github.com/emeryberger/Hoard/blob/master/src/include/hoard/geometricsizeclass.cpp
    static bool size2Class_precompute(class_t* sizes) {
      for(int i = 0; i <= maxSmallObjectSize; i++)
        sizes[i] = size2Class_search(i);
      return true;
    }

    static constexpr int size2Class_search(const size_t sz) {
      // Do a binary search to find the right size class.
      class_t left  = 0;
      class_t right = std::size(size_classes);
      while (left < right) {
        class_t mid = (left + right)/2;
        if (class2Size(mid) < sz) {
          left = mid + 1;
        } else {
          right = mid;
        }
      }
      deq_assert(class2Size(left) >= sz);
      deq_assert((left == 0) || (class2Size(left-1) < sz));
      return left;
    }

    SmallHeap smallHeaps[NumBins];
    LargeHeap largeHeap;

  public:

    enum { Alignment = gcd<LargeHeap::Alignment, SmallHeap::Alignment>::VALUE };

    inline void* malloc(const size_t sz) {
      if (sz <= maxSmallObjectSize) {
        const auto sizeClass = size2Class(sz);
        const auto realSize = class2Size(sizeClass);
        deq_assert(realSize >= sz);
        deq_assert(realSize <= maxSmallObjectSize);
        deq_assert(sizeClass >= 0);
        deq_assert(sizeClass < NumBins);
        return smallHeaps[sizeClass].malloc(realSize);
      } else {
        //large sizes do not have their size2Class precomputed, use slow version
        const auto sizeClass = size2Class_slow(sz);
        const auto realSize = class2Size(sizeClass);
        deq_assert(sizeClass > maxSmallObjectClass);
        return largeHeap.malloc(realSize);
      }
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
