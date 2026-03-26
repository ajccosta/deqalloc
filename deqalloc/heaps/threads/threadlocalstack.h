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

    struct alignas(128) thread_state {
      size_t sz = 0; //number of objects in this stack
      node_t* head = nullptr; //start of the stack
      node_t* mid = nullptr; //middle of the stack
      node_t* tail = nullptr; //tail of the stack
    };

    //Check that thread state fits in two cache lines
    static_assert(sizeof(thread_state) <= 128);

    thread_state thread_states[max_threads];
    inline thread_state& get_thread_state() { return thread_states[thread_id()]; }

  public:
    //Weird API (that includes list_length) because c++ makes it difficult
    // to instantiate multiple threadlocalstacks each with a different
    // list_length

    inline void* malloc(size_t sz, size_t list_length) {
      thread_state& ts = get_thread_state();
      if(ts.sz == 0) {
        auto [start_node, end_node] = Super::malloc(sz, list_length);
        ts.sz = list_length;
        ts.head = start_node;
        ts.tail = end_node;
        ts.mid = nullptr;
      }
      node_t* n = ts.head;
      ts.head = ts.head->next;
      ts.sz--;
      return static_cast<void*>(n);
    }

    inline void free(void* ptr, size_t sz, size_t list_length) {
      thread_state& ts = get_thread_state();
      if(ts.sz == 0) {
        ts.tail = static_cast<node_t*>(ptr);
      } else if(ts.sz == list_length+1) {
        ts.mid = ts.head;
      } else if(ts.sz == 2*list_length) {
#ifdef REMOTE_FREE
        Super::free(ts.mid->next, ts.tail, list_length);
#else
        Super::free(ts.mid->next, ts.tail);
#endif
        ts.tail = ts.mid;
        ts.mid->next = nullptr;
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