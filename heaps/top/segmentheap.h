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
#include "threads/epoch.h"
#include "threads/structures/HarrisLinkedList.h"
#include <atomic>
#include <limits>
#include <thread>

#include <iostream>

/**
 * @class SegmentHeap
 * @brief Heap that always allocates segments of the same size
 * @note Each SegmentHeap is for one size class only
 * @author André Costa
 */

#define DEFAULT_SEGMENT_SIZE 4 * 1024 * 1024 //4MiB


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

      static inline mapType MyMap;
      static inline PosixLockType MyMapLock;

      //Get the maximum number of objects in this segment
      static inline size_t getMaxNumObjects(size_t sz) {
        switch(getSegmentType(sz)) {
          case SegmentType::NORMAL:
            {
              size_t remaining_sz = SegmentSize - sizeof(header_t);
              size_t numObjects = remaining_sz / sz;
              return numObjects;
            }
          case SegmentType::SMALL_LARGE:
            return smallLargeSegmentNElems;
          case SegmentType::LARGE:
            return 1;
          default:
            std::cerr<<"Supposedly unreachable case statement reached."<<std::endl;
            std::abort();
        }
      }

      struct node_t {
        node_t* next;
      };

      struct header_t {
        //Auxiliary struct to allow pointer to list and number of nodes to be modified atomically
        struct alignas(8) list_size_t {
          private:
            //Number of bits for ABA tags
            static constexpr uint64_t tagbits = 16;
            static constexpr uint64_t tagmax = (1<<tagbits)-1;
            //Odd number of tag bits wastes a bit and confuses logic
            static_assert(tagbits % 2 == 0);

            //Number of bits to represent number of nodes and node indexes
            static constexpr uint64_t rest_bits = (64-tagbits)/2;
            static constexpr uint64_t restmax = (1<<rest_bits)-1;

            //MSB <tagbits> bits used for tag, then store num_nodes, then node_index
            //(verilog syntax)
            //[tagbits+(rest_bits*2)-1:(rest_bits*2)] tag
            //[(rest_bits*2)-1:rest_bits] num_nodes
            //[rest_bits-1:0] node_index
            uint64_t node_repr;

          public:
            list_size_t(uint64_t tag, uint64_t num_nodes, uint64_t node_index) :
              node_repr(getRepr(tag, num_nodes, node_index)) {}

            list_size_t() = default;

            static inline uint64_t node_index_null = restmax;

            inline uint64_t getTag() { return node_repr >> (64 - tagbits); }
            inline uint64_t getNumNodes() { return ((~(tagmax << (64 - tagbits))) & node_repr) >> rest_bits; }
            inline uint64_t getNodeIndex() { return restmax & node_repr; }
            inline std::tuple<uint64_t,uint64_t,uint64_t> getAll() { return {getTag(), getNumNodes(), getNodeIndex()}; }

          private:
            inline uint64_t getRepr(uint64_t tag, uint64_t num_nodes, uint64_t node_index) {
              return tagRepr(tag) | numNodesRepr(num_nodes) | nodeIndexRepr(node_index);
            }

            static inline uint64_t tagRepr(uint64_t tag) {
              //can be any value, it is supposed to loop
              return tag << (64 - tagbits);
            }

            static inline uint64_t numNodesRepr(uint64_t num_nodes) {
              assert(num_nodes <= restmax);
              return num_nodes << rest_bits;
            }

            static inline uint64_t nodeIndexRepr(uint64_t node_index) {
              return node_index;
            }
        };
        //list_size_t must fit in a word
        static_assert(sizeof(list_size_t) <= 8);
        static_assert(is_trivially_copyable_v<list_size_t>);
        static_assert(is_trivially_constructible_v<list_size_t>);
        static_assert(std::atomic<list_size_t>::is_always_lock_free);

        alignas(8) header_t* next_segment = nullptr; //linked list of segments of size sz
        header_t *segment_retire_next = nullptr;
        //TODO when sz is larger than SegmentSize we should treat differently?
        size_t sz; //size of nodes inside this segment
        std::atomic<list_size_t> head; //points to first element in node list

        //keep trying to pop node until list is empty
        void* popNode() {
          list_size_t h, new_h;
          node_t *node, *node_next;
          bool shouldfail;
          do {
            h = head.load(std::memory_order_relaxed);
            auto [tag, num_nodes, index] = h.getAll();
            assert(num_nodes >= 0 && num_nodes <= getMaxNumObjects(sz));
            if(num_nodes == 0) {
              return nullptr;
            }
            node = getNodePointer(getBase(), index, sz);
            assert(node != nullptr); //we checked for num_nodes=0 so this should never happen
            node_next = node->next;
            uint32_t new_index = getIndexFromNodePointer(getBase(), node_next, sz);
            shouldfail = !((new_index >= 0 && new_index <= getMaxNumObjects(sz))
              || new_index == list_size_t::node_index_null);
            new_h = list_size_t(tag+1, num_nodes-1, new_index);
          } while(!head.compare_exchange_strong(h, new_h));
          //CAS succeeded.
          //shouldfail is true if new_index pointed to memory outside of our segment.
          //This means that, before this thread was able to pop a node, another thread
          // succeeded and wrote something to that node. That's fine, but we should not
          // be able to pop that node, i.e., our CAS must fail.
          assert(!shouldfail);
          return node;
        }

        //returns the number of elements left in the list
        size_t pushNode(void* ptr) {
          //ptr is in segment list
          assert(getBase() + sizeof(header_t) <= ptr && ptr < getBase() + SegmentSize);
          uint64_t num_nodes_;
          node_t *node = (node_t*) ptr;
          uint64_t new_index = getIndexFromNodePointer(getBase(), node, sz);
          assert((new_index >= 0 && new_index <= getMaxNumObjects(sz)) ||
            new_index == list_size_t::node_index_null);
          list_size_t h, new_h;
          do {
            h = head.load(std::memory_order_relaxed);
            auto [tag, num_nodes, index] = h.getAll();
            num_nodes_ = num_nodes;
            //Pushing this node maintains invariant
            assert(num_nodes+1 >= 0 && num_nodes+1 <= getMaxNumObjects(sz));
            assert((index >= 0 && index <= getMaxNumObjects(sz))
              || index == list_size_t::node_index_null);
            new_h = list_size_t(tag+1, num_nodes+1, new_index);
            node->next = getNodePointer(getBase(), index, sz);
          } while(!head.compare_exchange_strong(h, new_h));
          return num_nodes_+1;
        }

        inline bool emptyList() {
          list_size_t old = head.load(std::memory_order_relaxed);
          auto [tag, num_nodes, index] = old.getAll();
          list_size_t list_size_t_null = list_size_t(tag+1, 0, 0);
          if(num_nodes == getMaxNumObjects(sz))
            return head.compare_exchange_strong(old, list_size_t_null);
          else
            return false; //Someone allocated from this list before we were able to empty it, abort
        }

        //get node pointer from segment base, node index and size of nodes
        static inline node_t* getNodePointer(void* base, uint32_t index, size_t sz) {
          if(index == list_size_t::node_index_null) return nullptr;
          return (node_t*)(((uintptr_t)base) + sizeof(header_t) + index * sz);
        }

        //get node index from segment base, node ptr and size of nodes
        static inline uint32_t getIndexFromNodePointer(void* base, node_t* nodeptr, size_t sz) {
          if(nodeptr == nullptr) return list_size_t::node_index_null;
          return (((uintptr_t)nodeptr) - (((uintptr_t)base) + sizeof(header_t)))/sz;
        }

        //returns base pointer to segment. assumes that header is in first bytes of segment
        inline void* getBase() {
          return this;
        }
      };

      //Insert segment into linked list of segments
      void insertSegment(header_t* header) {
        seglist.add(&header->next_segment);
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
      //This works as long as we maintain the invariant that only a single segment can have its
      // start address inside of a 8KB aligned region. This invariant can be satisfied if we never
      // allocate segments smaller than 8KB.
      // To see why this would be a problem, imagine that we allocated a segment (represented above) 
      // with 8KB, and its start address was 4KB, meaning the map would have the entry 0KB:4KB. Then,
      // if we allocate a 4KB segment, it is possible that that segment would be allocated in the region
      // 0KB-4KB, leading its map entry to be 0KB:0KB, conflicting with the 8KB segment.
      // Allocating larger segments is fine as long as all of its node's start address would fit inside
      // of a segment of size SegmentSize. This ensures that calling getBasePointer on those nodes will
      // hit the correct map entry.
      void* getBasePointer(void* ptr) {
        //The "larger" base (8KB in the above example)
        void* alignedBase_1 = getAlignedBase(ptr);
        //The "larger" base's end (12KB-1 in the above example)
        void* alignedBaseEnd_1 = getSegmentEnd(alignedBase_1);
        //The "smaller" base (4KB in the above example)
        void* alignedBase_2 = alignedBase_1 - SegmentSize;
        //The "smaller" base's end (8KB-1 in the above example)
        void* alignedBaseEnd_2 = getSegmentEnd(alignedBase_2);
        assert(alignedBase_2 == getAlignedBase(alignedBase_1-1));
        MyMapLock.lock();
        auto search = MyMap.find(alignedBase_1);
        void* segmentBaseAddr;
        if (search == MyMap.end()) {
          //Did not find segment in map, the correct segment 
          // is guaranteed to be aligned to alignedBase_2
          search = MyMap.find(alignedBase_2);
          assert(search != MyMap.end());
          segmentBaseAddr = search->second;
        } else {
          //Found a segment, but need to confirm it is the correct one
          segmentBaseAddr = search->second;
          //Does ptr belong inside this segment?
          if(!(segmentBaseAddr <= ptr && ptr < segmentBaseAddr + SegmentSize)) {
            //We did not find the correct segment, look at the alignedBase_2
            search = MyMap.find(alignedBase_2);
            assert(search != MyMap.end());
            segmentBaseAddr = search->second;
          }
        }
        //ptr belongs to segment list (i.e., fits inside segment and is not in the header)
        assert(segmentBaseAddr + sizeof(header_t) <= ptr && ptr < segmentBaseAddr + SegmentSize);
        MyMapLock.unlock();
        return segmentBaseAddr;
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

      //Round up to the size of a page.
      static constexpr size_t roundToPageSize(size_t sz) {
            return (sz + CPUInfo::PageSize - 1) & (size_t) ~(CPUInfo::PageSize - 1);
      }

      //sz: node size
      static inline size_t getActualSegmentSize(size_t sz) {
        switch(getSegmentType(sz)) {
          case SegmentType::NORMAL:
            return SegmentSize;
          case SegmentType::SMALL_LARGE:
            return roundToPageSize(sz * smallLargeSegmentNElems + sizeof(header_t));
          case SegmentType::LARGE:
            return roundToPageSize(sz + sizeof(header_t));
          default:
            std::cerr<<"Supposedly unreachable case statement reached."<<std::endl;
            std::abort();
        }
      }

      //Allocate and initialize new segment
      header_t* allocateSegment(size_t sz) {
        void* base = SizedMmapHeap::malloc(getActualSegmentSize(sz));
        size_t numObjects = getMaxNumObjects(sz);
        header_t* header = (header_t*) base; //use first bytes of segment as header
        header->sz = sz;
        //Initialize linked list
        initializeList(base + sizeof(header_t), sz, numObjects);
        typename header_t::list_size_t h = typename header_t::list_size_t(0, numObjects, 0);
        header->head.store(h);
        registerSegment(base);
        return header;
      }

      //Frees segment by removing it from MyMap and giving it back to SizedMmapHeap
      void freeSegment(header_t* header) {
        MyMapLock.lock();
        MyMap.erase(header);
        SizedMmapHeap::free(header, getActualSegmentSize(header->sz));
        MyMapLock.unlock();
      }

      //There are 3 types of Segments:
      //  - Normal segments (many nodes fit inside of a Segment), the segment has size SegmentSize
      //  - Small Large segments, at least one node fits inside of a segment with SegmentSize,
      //      but it would waste a lot of memory to have a single node there.
      //      For example, assume SegmentSize=2MB and sizeof(header_t)=32bytes, and we get a request
      //        for a 1MB allocation. We could only fit 1 1MB node inside a 2MB segment
      //      Thus, Small Large segments are larger than a SegmentSize but its nodes
      //        are smaller than SegmentSize. They have <smallLargeSementNElems> number of nodes.
      //  - Large Segments only have a single node
      enum SegmentType { NORMAL, SMALL_LARGE, LARGE };
      constexpr static size_t smallLargeSegmentNElems = 2;
      constexpr static size_t largestNormalSegSz = (SegmentSize - sizeof(header_t)) / smallLargeSegmentNElems;

      static constexpr SegmentType getSegmentType(size_t sz) {
        if (sz <= largestNormalSegSz) return SegmentType::NORMAL;
        if (sz < SegmentSize) return SegmentType::SMALL_LARGE;
        return SegmentType::LARGE;
      }

      //Empties segment, removes it from segment list and adds it to be retired later
      //Emptying the segment first is useful because we can assume that no other thread will
      //  allocate from it while it is in the retired list. Otherwise, before we were able to
      //  retire the segment, some other thread might have allocated from it. If that is the case,
      //  we cannot free the segment when the SMR method tells us it is safe to do so, we must wait
      //  for the segment to be full again. After it is full the segment can be free'd, because no
      //  other thread is able to allocate from it anymore. However, the thread that "filled" it last
      //  must check that the segment is full and that it has been retired, which means that both
      //  strategies incur some overhead. Emptying before retiring is simpler so we stick with that.
      void retireSegment(header_t* header) {
        if(!header->emptyList()) return;
        seglist.remove(&header->next_segment, [&](header_t* h){this->addToRetireList(h);});
      }

      struct thread_state {
        header_t* retire_current = nullptr; //segments added in current epoch e
        header_t* retire_old = nullptr; //segments in epoch e-1
        thread_state() {}
      };

      //Each thread keeps its own retire list of segments, per segment heap
      inline thread_state& get_thread_state() const  {
        thread_local __attribute__((tls_model("initial-exec"))) thread_state ts{};
        return ts;
      }

      void addToRetireList(header_t* header) {
        //Since we are guaranteed that only a thread can successfuly retire a segment,
        //  we can use the segment_retire_next field without synchronization.
        thread_state& ts = get_thread_state();
        //segment_retire_next should only be written once
        // in the entire lifetime of a segment (and by a single thread)
        assert(header->segment_retire_next == nullptr);
        header->segment_retire_next = ts.retire_current;
        ts.retire_current = header;
      }

      void advanceEpoch() {
        thread_state& ts = get_thread_state();
        //Reclaim safe
        header_t *h, *to_free;
        h = ts.retire_old; //old are now safe (epoch was advanced)
        while(h != nullptr){
          to_free = h;
          h = h->segment_retire_next;
          freeSegment(to_free);
        }
        //Move current to old
        ts.retire_old = ts.retire_current;
        //Empty current
        ts.retire_current = nullptr;
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

      inline void* malloc(size_t sz) {
        return uepoch::with_epoch([&] {
          header_t* header;
          void* p = nullptr;
          //TODO: look for non-empty segments to allocate from?
          header = seglist.peek();
          if(header != nullptr) //Segments available, try to pop from them
            p = header->popNode();
          if(p == nullptr) { //Need to allocate new segment
            header = allocateSegment(sz);
            p = header->popNode();
            insertSegment(header); //publish new segment
          }
          return reinterpret_cast<void*>(p);
        }, [&]{
          this->advanceEpoch();
        });
      }

      inline void free(void* ptr) {
        uepoch::with_epoch([&]{
          header_t* header = (header_t*) getBasePointer(ptr);
          size_t num_nodes = header->pushNode(ptr);
          assert(num_nodes <= getMaxNumObjects(header->sz));
          if(num_nodes == getMaxNumObjects(header->sz)) {
            //Segment is full again, attempt to retire it
            if(header != seglist.peek()) { //Don't deallocate the first segment
              retireSegment(header);
            }
          }
        }, [&]{
          this->advanceEpoch();
        });
      }

      //Can we just use a thread_local variable to keep track of the size?
      size_t getSize (void* ptr) {
        return uepoch::with_epoch([&]{
          header_t* header = (header_t*) getBasePointer(ptr);
          return header->sz;
        }, [&]{
          this->advanceEpoch();
        });
      }

    private:
      //linked list of segments
      //TODO: have full/partial/empty list to aid in searching for segments to allocate from?
      HarrisLinkedList<header_t*> seglist;
  };

}

#endif