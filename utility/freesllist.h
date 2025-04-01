// -*- C++ -*-

#ifndef HL_FREESLLIST_H_
#define HL_FREESLLIST_H_

#include <assert.h>

/**
 * @class FreeSLList
 * @brief A "memory neutral" singly-linked list,
 *
 * Uses the free space in objects to store
 * the pointers.
 */

//TODO: standard way of doing pointer arithmetic without ignoring warnings
#pragma GCC diagnostic ignored "-Wpointer-arith"

class FreeSLList {
public:

  FreeSLList& operator=(const FreeSLList &other) {
    head.next = other.head.next;
    return *this;
  }

  FreeSLList& operator=(const FreeSLList *other) {
    head = other->head;
    return *this;
  }

  inline void clear (void) {
    head.next = nullptr;
  }

  class Entry;
  
  //Get the head of the list.
  inline Entry * get (void) {
    const Entry * e = head.next;
    if (e == nullptr) {
      return nullptr;
    }
    head.next = e->next;
    return const_cast<Entry *>(e);
  }

  inline Entry * remove (void) {
    const Entry * e = head.next;
    if (e == nullptr) {
      return nullptr;
    }
    head.next = e->next;
    return const_cast<Entry *>(e);
  }
  
  inline void insert (void * e) {
    Entry * entry = reinterpret_cast<Entry *>(e);
    entry->next = head.next;
    head.next = entry;
  }

  inline bool isEmpty(void) {
    return head.next == nullptr;
  }

  //Each entry points to the one immediately after, except the last
  //Initializes list into buffer
  static FreeSLList* InitializeList(void *ptr, size_t sz, size_t numObjects) {
    for (size_t i = 0; i < numObjects; i++) {
      Entry* entry = reinterpret_cast<Entry*>(ptr + i * sz);
      entry->next = reinterpret_cast<Entry*>(ptr + (i+1) * sz);
    }
    return reinterpret_cast<FreeSLList*>(ptr);
  }

  class Entry {
  public:
    Entry (void) : next (nullptr) {}
    Entry * next;
  private:
    Entry (const Entry&);
  };
  
private:
  Entry head;
};


#endif




