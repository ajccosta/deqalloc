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

/**
 * @class ListHeap
 * @brief Connects Heaps that work in terms of lists to other types of Heaps.
 * malloc will return a list. This list may be acquired from a block allocated
 * from Super or it may be a list that had already been used and free'd, in which
 * case it will come from SuperList.
 * Super must be regular Heap that allocates blocks of memory.
 * [OPTIONAL] SuperList must be a Heap that also allocates/deallocates lists.
 * If no SuperList is given, all allocations will come from Super.
 * @author André Costa
 * Assumes calls to malloc always ask for the same size (and free's always return that size)
 * @warning TwoListHeap works for one "size class" only!
*/

#ifndef HL_LISTHEAP_H
#define HL_LISTHEAP_H

#include <assert.h>
#include "utility/freesllist.h"


namespace HL {

  //Does nothing with lists. Allows SuperList to be optional in parent class
  template <unsigned int numObjects_>
  class NoopSuperListHeap {
  public:
    static const unsigned int numObjects = numObjects_;
    FreeSLList* malloc([[maybe_unused]] size_t sz) {
      return nullptr;
    }
    bool free(FreeSLList* fl) {
      return false;
    }
  };

  template <
    unsigned int numObjects_,
    class Super,
    class SuperList = NoopSuperListHeap<numObjects_>>
  class ListHeap : public Super, SuperList {
  public:
    //SuperList must have lists of the same size
    static_assert(numObjects_ == SuperList::numObjects);
    static const unsigned int numObjects = numObjects_;

    ListHeap() {}

    ~ListHeap() { clear(); }

    inline FreeSLList* malloc(size_t sz) {
      assert(sz >= sizeof(void*)); //Cannot allocate nodes smaller then pointer types
      FreeSLList* fl = SuperList::malloc(sz);
      if(fl == nullptr) {
        void *ptr = Super::malloc(sz * numObjects);
        fl = FreeSLList::InitializeList(ptr, sz, numObjects);
      }
      return fl;
    }

    inline void free(FreeSLList* fl) {
      //TODO how to give nodes back to Super - the lists may be completely shuffled now
      SuperList::free(fl);
    }

    void clear() {
      //TODO
    }

  private:
    ListHeap (const ListHeap&);
    ListHeap& operator=(const ListHeap&);
  };
}

#endif
