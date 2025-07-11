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
#define DEFAULT_SMALL_SIZE_CLASS_MAX 32 * 1024 //32KiB

namespace HL {

  template<size_t SegmentSize_ = DEFAULT_SEGMENT_SIZE,
           size_t SmallSizeClassMax_ = DEFAULT_SMALL_SIZE_CLASS_MAX>
  class SegmentHeap : public AlignedMmapHeap {
    public:
      static constexpr size_t SegmentSize = SegmentSize_; //Make SegmentSize visible to other classes
      enum { Alignment = SegmentSize };

      static_assert(SegmentSize % CPUInfo::PageSize == 0 && "SegmentSize must be multiple of page size");
      static_assert((SegmentSize & (SegmentSize-1)) == 0 && "SegmentSize must be power of 2");
      static_assert(SegmentSize > 0 && "Naturally SegmentSize can't be 0");

      static constexpr size_t SmallSizeClassMax = SmallSizeClassMax_;

    public:
      //Get the maximum number of objects in this segment
      static inline constexpr size_t getMaxNumObjects(size_t sz) {
        switch(getSegmentType(sz)) {
          case SegmentType::NORMAL:
            {
              size_t remaining_sz = SegmentSize - sizeof(header_t);
              size_t numObjects = remaining_sz / sz;
              deq_assert(numObjects > 0);
              return numObjects;
            }
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

    private:

      struct alignas(64) header_t {
        //Auxiliary struct to allow pointer to list and number of nodes to be modified atomically
        struct alignas(8) list_size_t {
          private:
            //Number of bits for ABA tags
            static constexpr uint64_t tagbits = 21;
            static constexpr uint64_t tagmax = (1<<tagbits)-1;
            static constexpr uint64_t tagandcanonlbits = tagbits + 1; //tag + canonic bit
            //Odd number of tag bits wastes a bit and confuses logic
            static_assert(tagandcanonlbits % 2 == 0);

            //Number of bits to represent number of nodes and node indexes
            static constexpr uint64_t rest_bits = (64-tagandcanonlbits)/2;
            static constexpr uint64_t restmax = (1<<rest_bits)-1;

            //MSB <tagbits> bits used for tag, then store num_nodes, then node_index
            //(verilog syntax)
            //[64:63] canonic bit
            //[tagbits+(rest_bits*2)-2:(rest_bits*2)] tag
            //[(rest_bits*2)-1:rest_bits] num_nodes
            //[rest_bits-1:0] node_index
            uint64_t node_repr;

          public:
            list_size_t(bool canon, uint64_t tag, uint64_t num_nodes, uint64_t node_index) :
              node_repr(getRepr(canon, tag, num_nodes, node_index)) {}

            list_size_t() = default;

            static inline uint64_t node_index_null = restmax;

            //Whether the list is in a "canonic state", i.e., no pushes have been made yet
            inline uint64_t getCanon() const { return (node_repr & (1ull << 63))>>63; }
            inline uint64_t getTag() const { return (node_repr  & (~(1ull<<63))) >> (64 - tagandcanonlbits); }
            inline uint64_t getNumNodes() const { return ((~(((tagmax<<1)+1) << (64 - tagandcanonlbits))) & node_repr) >> rest_bits; }
            inline uint64_t getNodeIndex() const { return restmax & node_repr; }
            inline std::tuple<bool, uint64_t,uint64_t,uint64_t> getAll() { return {getCanon(), getTag(), getNumNodes(), getNodeIndex()}; }

            bool operator==(list_size_t const& rhs) const { return node_repr == rhs.node_repr; }
            bool operator!=(list_size_t const& rhs) const { return !(*this == rhs); }

          private:
            inline uint64_t getRepr(bool canon, uint64_t tag, uint64_t num_nodes, uint64_t node_index) {
              return canonRepr(canon) | tagRepr(tag) | numNodesRepr(num_nodes) | nodeIndexRepr(node_index);
            }

            static inline uint64_t canonRepr(bool canon) {
              deq_assert(((uint64_t)canon) == 0 || ((uint64_t)canon) == 1);
              return ((uint64_t)canon) << 63;
            }

            static inline uint64_t tagRepr(uint64_t tag) {
              //can be any value, it is supposed to loop
              //mask off canon bit
              return (tag << (64 - tagandcanonlbits)) & (~(1ull<<63));
            }

            static inline uint64_t numNodesRepr(uint64_t num_nodes) {
              deq_assert(num_nodes <= restmax);
              return num_nodes << rest_bits;
            }

            static inline uint64_t nodeIndexRepr(uint64_t node_index) {
              return node_index;
            }

            friend ostream& operator<<(ostream& os, list_size_t const& t) {
              return os << '{' <<
                t.getCanon() << ',' <<
                t.getTag() << ',' <<
                t.getNumNodes() << ',' <<
                t.getNodeIndex() << '}';
            }
        };
        //list_size_t must fit in a word
        static_assert(sizeof(list_size_t) <= 8);
        static_assert(is_trivially_copyable_v<list_size_t>);
        static_assert(is_trivially_constructible_v<list_size_t>);
        static_assert(std::atomic<list_size_t>::is_always_lock_free);

        alignas(8) header_t* next_segment = nullptr; //linked list of segments of size sz
        header_t *segment_retire_next = nullptr;
        std::atomic<list_size_t> head; //points to first element in node list

        struct size_threadlocal {
          alignas(8) size_t sz; //size of nodes inside this segment
        }
        size_threadlocal[max_threads];

        void* popNode() {
          auto [start, end, n_popped] = popNode(1);
          deq_assert(n_popped == 0 || n_popped == 1);
          deq_assert(start == end);
          return n_popped == 1 ? start : nullptr;
        }

        //(attempt to) allocate n nodes at a time
        //Returns linked list with
        //  {first node in list, last node in list, number of nodes in returned list}
        std::tuple<node_t*, node_t*, size_t> popNode(size_t n) {
          deq_assert(n > 0);
          checkList();
          list_size_t h, new_h;
          node_t *node, *start_node, *end_node;
          size_t n_popped; //how many nodes were popped
          bool canon;
          do {
popNode_n_retry:
            h = head.load(std::memory_order_relaxed);
            auto t = h.getAll();
            auto [_canon, tag, num_nodes, index] = t; //prevents vars from going out of scope
            canon = _canon;
            deq_assert(num_nodes >= 0 && num_nodes <= getMaxNumObjects(getSize()));
            if(num_nodes == 0) {
              deq_assert(index == list_size_t::node_index_null);
              return {nullptr, nullptr, 0};
            }
            //How many nodes to pop
            size_t n_pop = std::min(num_nodes, n);
            uint32_t new_index;
            if(!canon) { //list not in canonical representation, we have to traverse nodes
              deq_assert(n_pop > 0);
              int64_t n_traversed = n_pop; //How many nodes we will attempt to pop
              node = getNodePointer(index);
              start_node = node;
              while(n_traversed-- > 0) {
                end_node = node;
                //Other threads might have written to these locations concurrently.
                //They are only allowed to do so if they have allocated nodes first.
                //That means that even if we read their garbage, our CAS will fail.
                //Still, if we read their garbage and interpret it as a node, we segfault!
                //So, just check if what we read lies inside of this segment list, even
                //  if it is wrong, we won't segfault and will retry after a failed CAS.
                if(!belongsToSegmentList(node)) {
#ifndef NDEBUG
                //Head must have changed since someone wrote to this node
                std::atomic_thread_fence(std::memory_order_seq_cst);
                deq_assert(h != head.load(std::memory_order_relaxed));
#endif
                  goto popNode_n_retry;
                }
                node = node->next;
              }
              n_popped = (n_pop-(n_traversed+1));
              //If the node that will be the new head of the list does not belong to the list
              // and is not a nullptr (i.e., we are for sure reading something a user wrote)
              // we simply retry.
              //In the case that we see a nullptr (it is also considered to not be in the list)
              // we might be at the end of the list, in which case the nullptr is valid. In that
              // case n_popped==num_nodes. Otherwise, we saw another corrupting write (such as
              // truncating the end of the list in another thread), we need to retry as well.
              if((node == nullptr && n_popped != num_nodes)
                || (!belongsToSegmentList(node) && node != nullptr)) {
                goto popNode_n_retry;
              }
              new_index = getIndexFromNodePointer(node);
              deq_assert(belongsToSegmentList(node) || node == nullptr);
              deq_assert(node == nullptr ? (num_nodes-n_popped == 0 ||
                h != head.load(std::memory_order_seq_cst)): true);
            } else { //list is in canonical representation, don't traverse simply calculate next node
              start_node = getNodePointer(index);
              end_node = getNodePointer(index + n_pop - 1);
              n_popped = n_pop;
              new_index = n_pop == num_nodes ? list_size_t::node_index_null : index + n_pop;
            }
            deq_assert(num_nodes == n_popped ? new_index == list_size_t::node_index_null : true);
            new_h = list_size_t(canon, tag+1, num_nodes-n_popped, new_index);
          } while(!head.compare_exchange_strong(h, new_h));
          if(canon)
            initializeList(start_node, getSize(), n_popped);
          return {start_node, end_node, n_popped};
        }

        size_t pushNode(void* ptr) {
          return pushNode((node_t*)ptr, (node_t*)ptr, 1);
        }

        void checkList() {
#ifndef NDEBUG
          //Check that list is well-formed
          size_t retries = 0;
checkList_retry:
          list_size_t h = head.load(std::memory_order_seq_cst);
          auto t = h.getAll();
          auto [canon, tag, num_nodes, index] = t;
          if(canon) return;
          node_t* _c = getNodePointer(index);
          size_t _n_nodes = 0;
          while(_c) {
            _c = _c->next;
            _n_nodes++;
            if(!belongsToSegmentList(_c) && !(_c == nullptr && _n_nodes == num_nodes)) {
              deq_assert(retries++ < 10000);
              goto checkList_retry;
            }
          }
          deq_assert(_n_nodes == num_nodes);
#endif
        }

        //node_first: the first node in the list
        //node_last: the last node in the list
        //n_nodes: number of nodes in this list
        //if node_first == node_last the behavior is the same as pushing a single node
        //Afterwards, the list will look like:
        //  head -> node_first -> ... -> node_last -> prev_head -> ...
        //returns the number of elements left in the list
        size_t pushNode(node_t* node_first, node_t* node_last, size_t n_nodes) {
          //nodes belong in this segment's list
#ifndef NDEBUG
          //assert that all nodes belong to this segment and n_nodes is correct
          node_t* _c = node_first;
          size_t _n_nodes = 0;
          while(true) {
            _n_nodes++;
            deq_assert(belongsToSegmentList(_c));
            if(_c == node_last) break;
            _c = _c->next;
          }
          deq_assert(_n_nodes == n_nodes);
          checkList();
#endif
          uint64_t num_nodes_;
          uint64_t new_index = getIndexFromNodePointer(node_first);
          deq_assert((new_index >= 0 && new_index < getMaxNumObjects(getSize())) ||
            new_index == list_size_t::node_index_null);
          list_size_t h, new_h;
          do {
pushNode_n_retry:
            h = head.load(std::memory_order_relaxed);
            auto t = h.getAll();
            auto [canon, tag, num_nodes, index] = t;
            if(canon && num_nodes > 0) {
              list_size_t list_size_t_null = list_size_t(false, tag+1, 0, list_size_t::node_index_null);
              //try to empty list to get exclusive access to it
              bool emptied = head.compare_exchange_strong(h, list_size_t_null);
              if(!emptied) //failed emptying, retry
                goto pushNode_n_retry;
              //initialize rest of the list
              initializeList(getNodePointer(index), getSize(), num_nodes);
              //CAS will now try to change from empty list
              h = list_size_t_null;
            }
            num_nodes_ = num_nodes;
            new_h = list_size_t(false, tag+1, num_nodes+n_nodes, new_index);
            node_last->next = getNodePointer(index);
            deq_assert(num_nodes+n_nodes >= 0 && num_nodes+n_nodes <= getMaxNumObjects(getSize()));
            deq_assert((index >= 0 && index < getMaxNumObjects(getSize()))
              || index == list_size_t::node_index_null);
            deq_assert(num_nodes == 0 ? index == list_size_t::node_index_null : true);
            deq_assert(num_nodes == 0 ? node_last->next == nullptr : true);
          } while(!head.compare_exchange_strong(h, new_h));
          return num_nodes_+n_nodes;
        }

        inline bool emptyList() {
          list_size_t old = head.load(std::memory_order_relaxed);
          auto t = old.getAll();
          auto [canon, tag, num_nodes, index] = t;
          list_size_t list_size_t_null = list_size_t(canon, tag+1, 0, list_size_t::node_index_null);
          if(num_nodes == getMaxNumObjects(getSize()))
            return head.compare_exchange_strong(old, list_size_t_null);
          else
            return false; //Someone allocated from this list before we were able to empty it, abort
        }

        //Each thread gets its own cache line with sz.
        //This is an extreme waste but helps prevent cacheline
        // contention that was degrading performance significantly.
        inline size_t getSize() {
          size_t sz = size_threadlocal[thread_id()].sz;
          deq_assert(sz > 0);
          return sz;
        }

        node_t* getNodePointer(uint32_t index) {
          return getNodePointer(getBase(), index, getSize());
        }

        //get node pointer from segment base, node index and size of nodes
        static inline node_t* getNodePointer(void* base, uint32_t index, size_t sz) {
          if(index == list_size_t::node_index_null) return nullptr;
          return (node_t*)(((uintptr_t)base) + sizeof(header_t) + index * sz);
        }

        uint32_t getIndexFromNodePointer(node_t* nodeptr) {
          return getIndexFromNodePointer(getBase(), nodeptr, getSize());
        }

        //get node index from segment base, node ptr and size of nodes
        static inline uint32_t getIndexFromNodePointer(void* base, node_t* nodeptr, size_t sz) {
          if(nodeptr == nullptr) return list_size_t::node_index_null;
          return (((uintptr_t)nodeptr) - (((uintptr_t)base) + sizeof(header_t)))/sz;
        }

        //Checks if a pointer lies inside the bounds of this Segment's
        bool belongsToSegmentList(void* ptr) {
          return (getBase() + sizeof(header_t) <= ptr) && (ptr < getBase() + SegmentSize);
        }

        //returns base pointer to segment. assumes that header is in first bytes of segment
        inline void* getBase() {
          return this;
        }
      };

      //Largest small size class has to fit inside of a segment
      static_assert(SmallSizeClassMax <= (SegmentSize - sizeof(header_t)));

      //Insert segment into linked list of segments
      bool insertSegment(header_t* header, header_t* expected) {
        return seglist.compare_and_add(&header->next_segment, expected);
      }

      static inline void* getBasePointer(void* ptr) {
        return getAlignedBase(ptr);
      }

      //Returns the pointer to the SegmentSize aligned region which base belongs to
      static inline void* getAlignedBase(void* base) {
        void* ret = (void*)((uintptr_t) base & (~(SegmentSize-1)));
        deq_assert(reinterpret_cast<size_t>(ret) % SegmentSize == 0);
        return ret;
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
          case SegmentType::LARGE:
            return roundToPageSize(sz + sizeof(header_t));
          default:
            std::cerr<<"Supposedly unreachable case statement reached."<<std::endl;
            std::abort();
        }
      }

      //Allocate and initialize new segment
      header_t* allocateSegment(size_t sz) {
        void* base = AlignedMmapHeap::malloc(getActualSegmentSize(sz), SegmentSize);
        size_t numObjects = getMaxNumObjects(sz);
        header_t* header = (header_t*) base; //use first bytes of segment as header
        for(int i = 0; i < max_threads; i++)
          header->size_threadlocal[i].sz = sz;
        //Don't initialize linked list on segment creation, initialize as we go
        typename header_t::list_size_t h = typename header_t::list_size_t(true, 0, numObjects, 0);
        header->head.store(h);
        return header;
      }

      void freeSegment(header_t* header) {
        AlignedMmapHeap::free(header, getActualSegmentSize(header->getSize()));
      }

      enum SegmentType { NORMAL, LARGE };

      static constexpr SegmentType getSegmentType(size_t sz) {
        if (sz <= SmallSizeClassMax) return SegmentType::NORMAL;
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
        if(getMaxNumObjects(header->getSize()) > 1) { //Segment is shared, add to retire list
          if(!header->emptyList()) return;
          seglist.remove(&header->next_segment, [&](header_t* h){this->addToRetireList(h);});
        } else { //This segment is not shared, we are the only one to have a reference to it
          freeSegment(header);
        }
      }

      struct thread_state {
        header_t* retire_current = nullptr; //segments added in current epoch e
        header_t* retire_old = nullptr; //segments in epoch e-1
      };

      //Each thread keeps its own retire list of segments, per segment heap
      inline thread_state& get_thread_state() const {
        thread_local __attribute__((tls_model("initial-exec"))) thread_state ts{};
        return ts;
      }

      void addToRetireList(header_t* header) {
        //Since we are guaranteed that only a thread can successfuly retire a segment,
        //  we can use the segment_retire_next field without synchronization.
        thread_state& ts = get_thread_state();
        //segment_retire_next should only be written once
        // in the entire lifetime of a segment (and by a single thread)
        deq_assert(header->segment_retire_next == nullptr);
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

      //Joins two lists so that they become start_node1->...->end_node1->start_node2->...->end_node2
      //list 1 (start_node1->...->end_node1) might be empty, in which case start_node1 == nullptr
      //list 2 must be non-empty
      static std::pair<node_t*, node_t*> joinLists(node_t* start_node1, node_t* end_node1,
        node_t* start_node2, node_t* end_node2) {
        //returns the a new list
        node_t* start, *end;
        deq_assert(start_node2 != nullptr && end_node2 != nullptr);
        if(start_node1 == nullptr) {
          deq_assert(end_node1 == nullptr);
          start = start_node2;
        } else {//Join node lists
          start = start_node1;
          end_node1->next = start_node2;
        }
        end = end_node2;
        return {start, end};
      }

    public:

      //Allocate n blocks of size sz
      inline std::pair<node_t*, node_t*> malloc(size_t sz, size_t n) {
        //Don't try to allocate more than what
        //  fits inside a fully filled segment
        deq_assert(n <= getMaxNumObjects(sz));
        return uepoch::with_epoch([&]() -> std::pair<node_t*, node_t*> {
mallocN_retry_beginning:
          header_t* header;
          node_t *start_node = nullptr, *end_node = nullptr;
          int to_allocate = n;
mallocN_retry_partial:
          //TODO: look for non-empty segments to allocate from?
          header = seglist.peek();
          if(header != nullptr) {
            //Segments available, try to pop from them
            deq_assert(getSegmentType(sz) == SegmentType::NORMAL ? sz == header->getSize() : true);
            auto [_start_node, _end_node, allocated] = header->popNode(to_allocate);
            deq_assert(allocated <= n); //Don't allocate more than requested
            deq_assert(to_allocate >= 0);
            if(allocated > 0) {
              to_allocate -= allocated;
              auto [_start_node_j, _end_node_j] = joinLists(start_node, end_node, _start_node, _end_node);
              start_node = _start_node_j;
              end_node = _end_node_j;
            }
          }
          if(to_allocate > 0) { //Need to allocate new segment
            header_t* prev_header = header;
            header = allocateSegment(sz);
            //TODO make synchronization-free popNode. At this point only this thread has access to this segment
            //And the segment is fresh, so there is no need to actually traverse the list as this is deterministic
            auto [_start_node, _end_node, allocated] = header->popNode(to_allocate);
            //Nothing left to allocate
            deq_assert(to_allocate == allocated);
            to_allocate -= allocated;
            if(getMaxNumObjects(sz) > 1) { //Only publish segments that have other nodes to be shared
              bool inserted = insertSegment(header, prev_header); //publish new segment
              if(!inserted) {
                //Free segment and try again
                //TODO: have segment pool to avoid freeing and reallocting all the time
                freeSegment(header);
                if(n - allocated > 0) { //We have allocated a few nodes from the head segment, don't leak them
                  to_allocate += allocated; //Nodes allocated from this new segment were freed
                  goto mallocN_retry_partial;
                } else { //No nodes were allocated yet, just restart
                  goto mallocN_retry_beginning;
                }
              }
            }
            //Succeeded, join lists
            auto [_start_node_j, _end_node_j] = joinLists(start_node, end_node, _start_node, _end_node);
            start_node = _start_node_j;
            end_node = _end_node_j;
          }
          deq_assert(to_allocate == 0);
          end_node->next = nullptr;
          return {start_node, end_node};
        }, [&]{
          this->advanceEpoch();
        });
      }

      inline void* malloc(size_t sz) {
        return uepoch::with_epoch([&] {
malloc1_retry:
          header_t* header;
          void* p = nullptr;
          //TODO: look for non-empty segments to allocate from?
          header = seglist.peek();
          if(header != nullptr) {//Segments available, try to pop from them
            deq_assert(getSegmentType(sz) == SegmentType::NORMAL ? sz == header->getSize() : true);
            p = header->popNode();
          }
          if(p == nullptr) { //Need to allocate new segment
            header_t* prev_header = header;
            header = allocateSegment(sz);
            p = header->popNode();
            if(getMaxNumObjects(sz) > 1) { //Only publish segments that have other nodes to be shared
              bool inserted = insertSegment(header, prev_header); //publish new segment
              if(!inserted) {
                //Free segment and try again
                freeSegment(header);
                goto malloc1_retry;
              }
            }
          }
          return reinterpret_cast<void*>(p);
        }, [&]{
          this->advanceEpoch();
        });
      }

      inline void free(node_t* start, node_t* end) {
        //TODO FIX
        deq_assert(false); //Don't free to segmentheap, buggy interaction with thread local stack currently
        uepoch::with_epoch([&]{
          header_t* curr_header;
          node_t* curr, *curr_start, *curr_end;
          curr = start; //The current node we are looking at
          size_t curr_n_nodes; //Number of nodes in curr list
          while(true) {
            curr_header = (header_t*) getBasePointer(curr);
            curr_start = curr_end = curr;
            curr_n_nodes = 0;
            while(true) {
              //Does not belong to the same segment as the previous node(s)
              if(!curr_header->belongsToSegmentList(curr))
                break;
              curr_n_nodes++;
              curr_end = curr;
              if(curr == end) break;
              curr = curr->next;
            }
            size_t num_nodes = curr_header->pushNode(curr_start, curr_end, curr_n_nodes);
            if(num_nodes == getMaxNumObjects(curr_header->getSize())) {
              //Segment is full again, attempt to retire it
              if(curr_header != seglist.peek()) { //Don't deallocate the first segment
                retireSegment(curr_header);
              }
            }
            if(curr == end) break;
          }
        }, [&]{
          this->advanceEpoch();
        });
      }

      inline void free(void* ptr) {
        uepoch::with_epoch([&]{
          header_t* header = (header_t*) getBasePointer(ptr);
          size_t num_nodes = header->pushNode(ptr);
          deq_assert(num_nodes <= getMaxNumObjects(header->getSize()));
          if(num_nodes == getMaxNumObjects(header->getSize())) {
            //Segment is full again, attempt to retire it
            if(header != seglist.peek()) { //Don't deallocate the first segment
              retireSegment(header);
            }
          }
        }, [&]{
          this->advanceEpoch();
        });
      }

      //No need to be inside of an epoch. The getSize can only be called on a valid
      // pointer. A valid pointer mustn't be in a freed state (for our use). Thus,
      // the node this pointer points to must be holding up a Segment from being
      // deallocated (as a segment can only be allocated when all nodes inside it
      // are freed). Hence we are getSize is guaranteed to be safe to call on a pointer
      // that has not yet been freed.
      size_t getSize(void* ptr) {
        header_t* header = (header_t*) getBasePointer(ptr);
        return header->getSize();
      }

      static constexpr inline size_t SegmentNumNodes(size_t sz) {
        return getMaxNumObjects(sz);
      }

    private:
      //linked list of segments
      //TODO: have full/partial/empty list to aid in searching for segments to allocate from?
      HarrisLinkedList<header_t*> seglist;
  };

}

#endif