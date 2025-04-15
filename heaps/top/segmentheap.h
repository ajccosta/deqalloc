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
#include <atomic>

/**
 * @class SegmentHeap
 * @brief Heap that always allocates segments of the same size
 * @note Each SegmentHeap is for one size class only
 * @author André Costa
 */

//#define DEFAULT_SEGMENT_SIZE 4 * 1024 * 1024 //4MiB
#define DEFAULT_SEGMENT_SIZE 4 * 1024 //4MiB


namespace HL {

  template<size_t SegmentSize_ = DEFAULT_SEGMENT_SIZE>
  class SegmentHeap : public SizedMmapHeap {
    public:
      static const size_t SegmentSize = SegmentSize_; //Make SegmentSize visible to other classes
      enum { Alignment = CPUInfo::PageSize };

      //SegmentSize must be multiple of page size
      static_assert(SegmentSize % CPUInfo::PageSize == 0);
      //SegmentSize must be power of 2
      static_assert((SegmentSize & (SegmentSize-1)) == 0);
      //Naturally SegmentSize can't be 0
      static_assert(SegmentSize > 0);

    private:

      // Note: we never reclaim memory obtained for MyHeap, even when this heap is destroyed.
      class MyHeap : public LockedHeap<PosixLockType, FreelistHeap<BumpAlloc<65536, SizedMmapHeap>>> {};
      typedef std::unordered_map<void*, void*,
          std::hash<void*>, std::equal_to<void*>,
          HL::STLAllocator<std::pair<void* const, void* const>, MyHeap>>
        mapType;

      mapType MyMap;
      PosixLockType MyMapLock;

      //Get the maximum number of objects in this segment
      static inline size_t getNumObjects(size_t sz) {
        size_t remaining_sz = SegmentSize - sizeof(header_t);
        size_t numObjects = remaining_sz / sz;
        return numObjects;
      }

      struct node_t {
        node_t* next;
      };

      struct header_t {
        //Auxiliary struct to allow pointer to list and number of nodes to be modified atomically
        struct alignas(8) list_size_t {
          uint32_t num_nodes; //How many nodes are left in this segment
          uint32_t node_index; //What is the index of this node inside the segment
        };
        //list_size_t must fit in a word
        static_assert(sizeof(list_size_t) <= 8);

        header_t* next_segment; //linked list of segments of size sz
        //TODO when sz is larger than SegmentSize we should treat differently?
        size_t sz; //size of nodes inside this segment
        std::atomic<list_size_t> head; //points to first element in node list

        //keep trying to pop node until list is empty
        void* popNode() {
          list_size_t h, new_h;
          node_t *node, *node_next;
          do {
            h = head.load(std::memory_order_relaxed);
            auto [num_nodes, index] = h;
            if(num_nodes == 0) {
              node = nullptr;
              break;
            }
            node = getNodePointer(getBase(), index, sz);
            node_next = node->next;
            uint32_t new_index = getIndexFromNodePointer(getBase(), node_next, sz);
            new_h = list_size_t{num_nodes-1, new_index};
          } while(!head.compare_exchange_strong(h, new_h));
          return node;
        }

        //returns the number of elements left in the list
        size_t pushNode(void* ptr) {
          uint32_t num_nodes;
          node_t *node = (node_t*) ptr;
          uint32_t new_index = getIndexFromNodePointer(getBase(), node, sz);
          list_size_t h, new_h;
          do {
            h = head.load(std::memory_order_relaxed);
            auto [num_nodes, index] = h;
            new_h = list_size_t{num_nodes+1, new_index};
            node->next = getNodePointer(getBase(), index, sz);
          } while(!head.compare_exchange_strong(h, new_h));
          return num_nodes+1;
        }

        inline bool emptyList() {
          list_size_t old = head.load(std::memory_order_relaxed);
          list_size_t list_size_t_null = {0,0};
          return head.compare_exchange_strong(old, list_size_t_null);
        }

        //get node pointer from segment base, node index and size of nodes
        static inline node_t* getNodePointer(void* base, uint32_t index, size_t sz) {
          return (node_t*)(((uintptr_t)base) + sizeof(header_t) + index * sz);
        }

        //get node index from segment base, node ptr and size of nodes
        static inline uint32_t getIndexFromNodePointer(void* base, node_t* nodeptr, size_t sz) {
          return (((uintptr_t)nodeptr) - (((uintptr_t)base) + sizeof(header_t)))/sz;
        }

        //returns base pointer to segment. assumes that header is in first bytes of segment
        inline void* getBase() {
          return this;
        }
      };

      void insertSegment(header_t* header) {
        header_t * curr;
        do { //insert new segment at head of the segment list
          curr = seglist.load(std::memory_order_relaxed);
          header->next_segment = curr;
        } while(!seglist.compare_exchange_strong(curr,
            header,
            std::memory_order_seq_cst));
      }


      //Register newly created segment in map
      //Base: pointer to first byte in segment; sz: size of segment
      void registerSegment(void* base) {
        void* alignedBase = getAlignedBase(base);
        MyMapLock.lock();
        MyMap[alignedBase] = base;
        MyMapLock.unlock();
      }

      //Finds base pointer of which ptr belongs to
      //When a segment's base pointer is not aligned to the segment size (likely)
      // ptr may be in either the first or second aligned region of its segment
      //For example, the following represents a 8KB segment that is only 4KB aligned
      // 0   4KB  8KB  12KB
      // [    |xxxx|xxxx]
      //      ^ segment base
      //              ^ ptr
      //In this case, MyMap contains the entry 0KB:4KB
      //If ptr belongs to the segment from 4KB-12KB, it is not enough
      // to search for the 8KB aligned part of ptr (masking the last 13 LSbits
      // which will search MyMap for the 8KB key).
      //Therefore we may also search MyMap for the previous 8KB aligned segment.
      //Note that there may be a segments with the mapping 8KB:X (e.g., a segment starting at 12KB),
      // which means we must check if ptr belongs to that segment. The "previous" segment mapping is
      // guarateed to be unique because we always allocate segments of the same size and it is impossible
      // to fit another 8KB segment between the range 0KB-4KB.
      void* getBasePointer(void* ptr) {
        void* alignedBase_1 = getAlignedBase(ptr);
        void* alignedBaseEnd_1 = getSegmentEnd(alignedBase_1);
        void* alignedBase_2 = alignedBase_1 - SegmentSize;
        void* alignedBaseEnd_2 = getSegmentEnd(alignedBase_1);
        assert(alignedBase_2 == getAlignedBase(alignedBase_1-1));
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

      //Initializes node list in this segment
      //ptr will point to the first element in the list
      static node_t* initializeList(void *ptr, size_t sz, size_t numObjects) {
        //Elements point to each other in a sequence
        for (size_t i = 0; i < numObjects - 1; i++) {
          node_t* n = reinterpret_cast<node_t*>(ptr + i * sz);
          n->next = reinterpret_cast<node_t*>(ptr + (i+1) * sz);
        }
        //Tail is null
        node_t* n = reinterpret_cast<node_t*>(ptr + (numObjects-1) * sz);
        n->next = nullptr;
        return reinterpret_cast<node_t*>(ptr);
      }

      //Allocate and initialize new segment
      header_t* allocateSegment(size_t sz) {
        void* base = SizedMmapHeap::malloc(SegmentSize);
        header_t* header = (header_t*) base; //use first bytes of segment as header
        header->sz = sz;
        //Initialize linked list
        size_t numObjects = getNumObjects(sz);
        initializeList(base + sizeof(header_t), sz, numObjects);
        typename header_t::list_size_t h = {(uint32_t)numObjects, 0};
        header->head.store(h);
        registerSegment(base);
        return header;
      }

      //Frees segment starting at base
      void freeSegment(void* base) {
        SizedMmapHeap::free(base, SegmentSize);
      }

      //Returns the last address of segment whose start is base
      static inline void* getSegmentEnd(void* base) {
        assert(base == getAlignedBase(base));
        return base + SegmentSize - 1;
      }

      //Returns the pointer to the SegmentSize aligned region which base belongs to
      static inline void* getAlignedBase(void* base) {
        void* ret = (void*)((uintptr_t) base & (~(SegmentSize-1)));
        assert(reinterpret_cast<size_t>(ret) % SegmentSize == 0);
        return ret;
      }

    public:

      inline void* malloc(size_t sz) {
        header_t* header;
        void* p = nullptr;
        //TODO: look for non-empty segments to allocate from?
        header = seglist.load(std::memory_order_relaxed);
        if(header != nullptr) //Segments available, try to pop from them
          p = header->popNode();
        if(p == nullptr) { //Need to allocate new segment
          header = allocateSegment(sz);
          p = header->popNode(); 
          insertSegment(header); //publish new segment
        }
        return reinterpret_cast<void*>(p);
      }

      inline void free(void* ptr) {
        header_t* header = (header_t*) getBasePointer(ptr);
        size_t num_nodes = header->pushNode(ptr);
        if(num_nodes == getNumObjects(header->sz)) {
          //Segment is full again, attempt to deallocate it
          //First, make sure no one else allocates from this segment
          if(header->emptyList()) {
            freeSegment(header);
          } // else, someone else changed list first, give up
        }
      }

    private:
      //linked list of segments
      //TODO: have full/partial/empty list to aid in searching for segments to allocate from?
      std::atomic<header_t*> seglist {nullptr};
  };

}

#endif