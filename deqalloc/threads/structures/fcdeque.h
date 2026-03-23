#pragma once

#include <cassert>
#include <atomic>
#include <deque>
#include <mutex>
#include <optional>
#include <vector>

#include "../threadmanager.h"

// Padding struct
template<size_t N>
struct Pad { char data[N]; };

// Specializtion for zero: empty struct
template<>
struct Pad<0> {};

// A concurrent deque based on flat combining
template<typename T, size_t MaxThreads = max_threads>
class FCDeque {
public:
  FCDeque() :
    heap{},
    backing(Allocator<T>(heap)),
    threadRequests(MaxThreads, Allocator<Request>(heap))
  {}

  void push_back(T value) {
    threadRequests[thread_id()].set(value, Request::PushBack);
    wait();
  }

  void push_front(T value) {
    threadRequests[thread_id()].set(value, Request::PushFront);
    wait();
  }

  std::optional<T> pop_back() {
    size_t tid = thread_id();
    threadRequests[tid].setStatus(Request::PopBack);
    wait();
    return threadRequests[tid].value;
  }

  std::optional<T> pop_front() {
    size_t tid = thread_id();
    threadRequests[tid].setStatus(Request::PopFront);
    wait();
    return threadRequests[tid].value;
  }

  std::pair<std::optional<T>, bool> pop_top() {
    return {pop_front(), false};
  }

  auto pop_bottom() {
    return pop_back();
  }

  void push_bottom(T val) {
    push_back(val);
  }

private:
  struct alignas(64) Request {
    enum Status {
      PushBack,
      PushFront,
      PopBack,
      PopFront,
      Complete
    };
    std::optional<T> value;
    std::atomic<Status> status;
    Request() : status{Complete} {}
    void set(T value, Status status)
    {
      this->value = value;
      setStatus(status);
    }
    void setStatus(Status status) {
      this->status.store(status, std::memory_order_release);
    }
    Status getStatus(std::memory_order mo = std::memory_order_acquire) const {
      return status.load(mo);
    }
    bool isComplete(std::memory_order mo = std::memory_order_acquire) const {
      return getStatus(mo) == Complete;
    }
    bool isPending(std::memory_order mo = std::memory_order_acquire) const {
      return !isComplete(mo);
    }
    void reset() {
      value.reset();
      setStatus(Complete);
    }
    void complete(T value) {
      set(value, Complete);
    }

    static constexpr size_t padding()
    {
      constexpr size_t used = sizeof(value) + sizeof(status);
      constexpr size_t rem = used % 64;
      return rem == 0 ? 0 : 64 - rem;
    }
    Pad<padding()> pad;
  };

  static_assert(sizeof(Request) % 64 == 0, "Request must be cache-line sized");

  static constexpr size_t BLOCK_SIZE_LOG = 12;
  using Heap = FreelistHeap<BumpAlloc<BLOCK_SIZE_LOG, SizedMmapHeap>>;

  template<typename V>
  struct Allocator {
    using value_type = V;
    Heap &heap;

    Allocator(Heap &heap) noexcept : heap(heap) {}

    template<typename U>
    Allocator(const Allocator<U>& other) noexcept : heap(other.heap) {}

    V *allocate(size_t sz) {
      return static_cast<V*>(heap.malloc(sz * sizeof(V)));
    }
    void deallocate(V *ptr, size_t) {
      heap.free(ptr);
    }

    template<typename U>
    bool operator==(const Allocator<U>& other) const noexcept {
      return &heap == &other.heap;
    }

    template<typename U>
    bool operator!=(const Allocator<U>& other) const noexcept {
      return !(*this == other);
    }
  };

  void combine() {
    size_t threadCount = num_threads();
    for (size_t i = 0; i < threadCount; i++) {
      auto &request = threadRequests[i];
      switch (request.getStatus()) {
        case Request::PushFront: {
          backing.push_front(*request.value);
          request.reset();
          break;
        }
        case Request::PushBack: {
          backing.push_back(*request.value);
          request.reset();
          break;
        }
        case Request::PopFront: {
          if (backing.empty()) {
            request.reset();
          } else {
            request.complete(backing.front());
            backing.pop_front();
          }
          break;
        }
        case Request::PopBack: {
          if (backing.empty()) {
            request.reset();
          } else {
            request.complete(backing.back());
            backing.pop_back();
          }
          break;
        }
        case Request::Complete:
          // do nothing
          break;
      }
    }
  }

  void wait() {
    size_t tid = thread_id();
    std::unique_lock<std::mutex> lock(mutex, std::defer_lock);
    while (threadRequests[tid].isPending()) {
      if (lock.try_lock()) {
        combine();
        return;
      }
    }
  }

  Heap heap;
  std::deque<T, Allocator<T>> backing;
  std::mutex mutex;
  std::vector<Request, Allocator<Request>> threadRequests;

};
