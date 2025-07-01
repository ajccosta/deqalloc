#ifndef THREAD_LOCAL_STACK_H
#define THREAD_LOCAL_STACK_H

/**
 * @class ThreadLocalStack
 * @brief Heap that uses a thread-local stack for allocation
 * This is an overspecialization, we could use the more generic
 *  ThreadSpecificHeap<TwoListHeap> but that would likely require
 *  runtime checks to guarantee that the thread_local variable is
 *  initialized. In ThreadLocalStack we use only thread_local variables
 *  with trivial types, so that no runtime checks are needed on every
 *  access.
 * @note Each ThreadLocalStack is for one size class only
 * @author André Costa
 */

template <class Super = DequeHeap<>>
class ThreadLocalStack : public Super {
  public:

    enum { Alignment = Super::Alignment };
    using node_t = typename Super::node_t;

  private:

    struct alignas(64) thread_state {
      //reads/writes to aligned variables are faster, hence alignas(8)
      alignas(8) size_t sz = 0; //number of objects in this stack
      alignas(8) node_t* head = nullptr; //start of the stack
      alignas(8) node_t* mid = nullptr; //middle of the stack
      alignas(8) node_t* tail = nullptr; //tail of the stack
    };

    //Check that thread state fits in single a cache line
    static_assert(sizeof(thread_state) <= 64);

    thread_state thread_states[max_threads];
    inline thread_state& get_thread_state() { return thread_states[thread_id()]; }


    //static inline constexpr size_t default_list_bytes = 1ul << 14; // in bytes
    static inline constexpr size_t default_list_bytes = 32*1024-64; // in bytes


    //TODO don't calculate list_length every time
    static constexpr size_t get_list_length(size_t sz /*size of objects*/) {
      size_t s1 = default_list_bytes / sz;
      size_t s2 = s1 < 1 ? 1 : s1; //minimum list_length of 2;
      return s2;
    }

    //Number of nodes in threadlocalstack exceeds number of nodes in a single segment
    static_assert(Super::SegmentNumNodes(32) >= get_list_length(32));

  public:

    inline void* malloc(size_t sz) {
      thread_state& ts = get_thread_state();
      const size_t list_length = get_list_length(sz);
      if(ts.sz == 0) {
        auto [start_node, end_node] = Super::malloc(sz, list_length);
        ts.head = start_node;
        ts.tail = end_node;
        ts.mid = nullptr;
        ts.sz = list_length;
      }
      node_t* n = ts.head;
      ts.head = ts.head->next;
      ts.sz--;
      return static_cast<void*>(n);
    }

    inline void free(void* ptr, size_t sz) {
      thread_state& ts = get_thread_state();
      const size_t list_length = get_list_length(sz);
      if(ts.sz == list_length+1) {
        ts.mid = ts.head;
      } else if(ts.sz == 2*list_length) {
        Super::free(ts.mid->next, ts.tail);
        ts.tail = ts.mid;
        ts.mid->next = nullptr;
        //ts.mid = nullptr;
        ts.sz = list_length;
      }
      node_t* n = new (ptr) node_t{ts.head};
      ts.head = n;
      ts.sz++;
    }

    inline size_t getSize(void *ptr) {
      return Super::getSize(ptr);
    }
};

#endif