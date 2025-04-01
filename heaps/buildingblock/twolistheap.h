/* -*- C++ -*- */

#ifndef HL_TWOLISTHEAP_H_
#define HL_TWOLISTHEAP_H_

/**
 * @class TwoListHeap
 * @brief Keeps free list up to 2 * numObjects in size.
 * Lists of size numObjects are simultaneously malloc'd/free'd from/to the SuperList Heap.
 * One of the list is free'd when both lists combined have 2 * numObjects.
 * @author André Costa
 * @warning TwoListHeap works for one "size class" only!
 * SuperList must be a class that also works in terms of Lists.
 */

#include "utility/freesllist.h"

template <int numObjects, class SuperList>
class TwoListHeap : public SuperList {
public:

  //SuperList must have lists of the same size
  static_assert(numObjects == SuperList::numObjects);

  TwoListHeap() : nObjects (0) {}

  ~TwoListHeap() { clear(); }

  inline void * malloc(size_t sz) {
    void *ptr;
    if(nObjects == 0) {
      _freelist = SuperList::malloc(sz);
      nObjects = numObjects;
    }
    ptr = _freelist.get();
    assert(ptr != nullptr);
    nObjects--;
    return ptr;
  }

  inline void free(void * ptr) {
    if (nObjects == numObjects+1) {
      _freelist_mid = _freelist;
    } else if (nObjects == numObjects * 2) {
      SuperList::free(&_freelist_mid);
      _freelist_mid.clear();
      nObjects = numObjects;
    }
    _freelist.insert(ptr);
    nObjects++;
  }

  //Clears to SuperList
  //Should only be called on shutdown
  inline void clear(void) {
    //TODO: lists may not be completely full, i.e., they may have less than numObjects
    //  This may cause things to break in SuperList
    //  if it assumes that lists always have numObjects objects.
    if(!_freelist.isEmpty()) {
      SuperList::free(&_freelist);
      _freelist.clear();
    }
    if(!_freelist_mid.isEmpty()) {
      SuperList::free(&_freelist_mid);
      _freelist_mid.clear();
    }
    nObjects = 0;
  }

private:

  int nObjects;
  FreeSLList _freelist;
  FreeSLList _freelist_mid;
};

#endif