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

#ifndef DQ_MMAPHEAP_H
#define DQ_MMAPHEAP_H

#if defined(_WIN32)
#include <windows.h>
#else
// UNIX
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <map>
#endif

#include <new>
#include <unordered_map>

#include "threads/cpuinfo.h"
#include "wrappers/mmapwrapper.h"
#include "wrappers/stlallocator.h"

#ifndef DQ_MMAP_PROTECTION_MASK
#if DQ_EXECUTABLE_HEAP
#define DQ_MMAP_PROTECTION_MASK (PROT_READ | PROT_WRITE | PROT_EXEC)
#else
#define DQ_MMAP_PROTECTION_MASK (PROT_READ | PROT_WRITE)
#endif
#endif


#if !defined(MAP_ANONYMOUS) && defined(MAP_ANON)
#define MAP_ANONYMOUS MAP_ANON
#endif

namespace deqalloc {

/**
 * @class SizedMmapHeap
 * @brief A heap around mmap, but only a sized-free is supported for Unix-like systems.
 */
class SizedMmapHeap {
public:

  /// All memory from here is zeroed.
  enum { ZeroMemory = 1 };

  enum { Alignment = HL::MmapWrapper::Alignment };

#if defined(_WIN32) 

  static inline void * malloc (size_t sz) {
#if DQ_EXECUTABLE_HEAP
    char * ptr = (char *) VirtualAlloc (NULL, sz, MEM_RESERVE | MEM_COMMIT | MEM_TOP_DOWN, PAGE_EXECUTE_READWRITE);
#else
    char * ptr = (char *) VirtualAlloc (NULL, sz, MEM_RESERVE | MEM_COMMIT | MEM_TOP_DOWN, PAGE_READWRITE);
#endif
    return (void *) ptr;
  }

  static inline void free (void * ptr, size_t) {
    // No need to keep track of sizes in Windows.
    VirtualFree (ptr, 0, MEM_RELEASE);
  }

  static inline void free (void * ptr) {
    // No need to keep track of sizes in Windows.
    VirtualFree (ptr, 0, MEM_RELEASE);
  }

  inline static size_t getSize (void * ptr) {
    MEMORY_BASIC_INFORMATION mbi;
    VirtualQuery (ptr, &mbi, sizeof(mbi));
    return (size_t) mbi.RegionSize;
  }

#else

  static inline void * malloc (size_t sz) {
    // Round up to the size of a page.
    sz = (sz + HL::CPUInfo::PageSize - 1) & (size_t) ~(HL::CPUInfo::PageSize - 1);
    void * addr = 0;
    int flags = 0;
    static int fd = -1;
#if defined(MAP_ALIGN) && defined(MAP_ANON)
    addr = (void *)Alignment;
    flags |= MAP_PRIVATE | MAP_ALIGN | MAP_ANON;
#elif !defined(MAP_ANONYMOUS)
    if (fd == -1) {
d = ::open ("/dev/zero", O_RDWR);
    }
    flags |= MAP_PRIVATE;
#else
    flags |= MAP_PRIVATE | MAP_ANONYMOUS;
#if DQ_EXECUTABLE_HEAP
#if defined(MAP_JIT)
    flags |= MAP_JIT;
#endif
#endif
#endif

    auto ptr = mmap (addr, sz, DQ_MMAP_PROTECTION_MASK, flags, fd, 0);
    if (ptr == MAP_FAILED) {
     ptr = nullptr;
    }
    return ptr;
  }
  
  static void free (void * ptr, size_t sz) {
    munmap(reinterpret_cast<char *>(ptr), sz);
  }

#endif

};

class AlignedMmapHeap : public SizedMmapHeap {

  public:
    static constexpr int OS_DECIDES_HUGE_PAGES = 0;
    static constexpr int NO_HUGE_PAGES = 1;
    static constexpr int USE_HUGE_PAGES = 2;

    static inline void* malloc (size_t sz, size_t alignment, int use_huge_pages = 0) {
      assert(alignment % HL::CPUInfo::PageSize == 0 && "alignment must be multiple of page size");
      assert((alignment & (alignment-1)) == 0 && "alignment must be power of 2");
      //allocate more than we need to
      void* ptr;
      ptr = SizedMmapHeap::malloc(sz + alignment);
      if(ptr == nullptr) return nullptr;
      uintptr_t p = (uintptr_t) ptr;
      //address aligned to <alignment>
      uintptr_t aligned_addr = (p + alignment - 1) & ~(alignment - 1);
      if(use_huge_pages == 1) madvise((void*)aligned_addr, sz, MADV_NOHUGEPAGE);
      else if(use_huge_pages == 2) madvise((void*)aligned_addr, sz, MADV_HUGEPAGE);
      assert(aligned_addr >= p);
      size_t prefix_sz = aligned_addr - p;
      size_t suffix_sz = alignment - prefix_sz;
      if(prefix_sz > 0) SizedMmapHeap::free((void*)p, prefix_sz);
      if(suffix_sz > 0) SizedMmapHeap::free((void*)((uintptr_t)aligned_addr + sz), suffix_sz);
      return (void*)aligned_addr;
    }
};

}

#endif
