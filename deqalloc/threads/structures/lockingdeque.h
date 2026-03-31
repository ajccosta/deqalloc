#pragma once

#include <cassert>
#include <atomic>
#include <deque>
#include <mutex>
#include <optional>
#include <vector>

#include "../threadmanager.h"

// Synchronizes via a global lock
template<typename T>
class alignas(64) LockingDeque {
private:
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
  };

public:
  LockingDeque() :
    heap{},
    backing(Allocator<T>(heap))
  {}

  std::optional<T> pop_front() {
    std::unique_lock lock(mutex);
    if (backing.empty())
      return std::nullopt;
    auto val = backing.front();
    backing.pop_front();
    return val;
  }

  std::optional<T> pop_back() {
    std::unique_lock lock(mutex);
    if (backing.empty())
      return std::nullopt;
    auto val = backing.back();
    backing.pop_back();
    return val;
  }

  std::pair<std::optional<T>, bool> pop_top() {
    return {pop_front(), false};
  }

  void push_top_direct(T val) {
    std::unique_lock lock(mutex);
    backing.push_front(val);
  }

  void push_bottom_direct(T val) {
    std::unique_lock lock(mutex);
    backing.push_back(val);
  }

  void push_top(T val) {
    push_top_direct(val);
  }

  auto pop_bottom() {
    return pop_back();
  }

  void push_bottom(T val) {
    push_bottom_direct(val);
  }

  Heap heap;
  std::deque<T, Allocator<T>> backing;
  std::mutex mutex;

};
