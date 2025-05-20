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

    inline thread_state& get_thread_state() const {
      //TODO check assembled code verify that this incurs no runtime checks
      thread_local __attribute__((tls_model("initial-exec"))) thread_state ts{};
      return ts;
    }

  public:

    static constexpr size_t list_length = 4; //TODO THIS SHOULD BE ADJUSTABLE PER SIZE CLASS!!!!!

    inline void* malloc(size_t sz) {
      thread_state& ts = get_thread_state();
      if(ts.sz == 0) {
        auto [start_node, end_node] = Super::malloc(sz, list_length);
        deq_assert(start_node != nullptr && end_node != nullptr);
        ts.head = start_node;
        ts.tail = end_node;
        ts.sz = list_length;
      }
      node_t* n = ts.head;
      deq_assert(n != nullptr);
      ts.head = ts.head->next;
      ts.sz--;
      return n;
    }

    inline void free(void* ptr) {
      thread_state& ts = get_thread_state();
      node_t* n = (node_t*) ptr;
      deq_assert(n != nullptr);
      if(ts.sz == list_length+1) {
        deq_assert(ts.head != nullptr);
        ts.mid = ts.head;
      } else if(ts.sz == 2*list_length) {
        deq_assert(ts.mid->next != nullptr && ts.tail != nullptr);
        Super::free(ts.mid->next, ts.tail);
        ts.tail = ts.mid;
        ts.mid->next = nullptr;
        ts.sz = list_length;
      }
      n->next = ts.head;
      ts.head = n;
      ts.sz++;
    }

    inline size_t getSize(void *ptr) {
      return Super::getSize(ptr);
    }
};

#endif