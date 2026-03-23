#pragma once

#include <cassert>
#include <atomic>
#include <deque>
#include <mutex>
#include <optional>
#include <vector>

#include "../threadmanager.h"

struct Allocator {
  static constexpr size_t BLOCK_SIZE_LOG = 12;
  using Base = FreelistHeap<BumpAlloc<BLOCK_SIZE_LOG, SizedMmapHeap>>;
  static inline Base allocator;
  static void *malloc(size_t sz) {
    return allocator.malloc(sz);
  }
  static void free(void *ptr) {
    allocator.free(ptr);
  }
};

template<size_t N>
struct Pad { char data[N]; };

template<>
struct Pad<0> {};

template<typename T>
class FCDeque {
public:
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
    void set(Status status)
    {
      value.reset();
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
      set(Complete);
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

  FCDeque() : threadRequests(max_threads) {}

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
    threadRequests[tid].set(Request::PopBack);
    wait();
    return threadRequests[tid].value;
  }

  std::optional<T> pop_front() {
    size_t tid = thread_id();
    threadRequests[tid].set(Request::PopFront);
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

  static constexpr size_t BLOCK_SIZE_LOG = 12;

  template<typename V>
  using STLAllocator = HL::STLAllocator<V, Allocator>;

  std::deque<T, STLAllocator<T>> backing;
  std::mutex mutex;
  std::vector<Request, STLAllocator<Request>> threadRequests;

};
