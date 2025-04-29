#ifndef PARLAY_INTERNAL_RANDOM_H_
#define PARLAY_INTERNAL_RANDOM_H_

// From parlaylib: https://github.com/cmuparlay/parlaylib
//  random.h
//  utilities.h

#include <limits> 

namespace parlay {

// from numerical recipes
inline uint64_t hash64(uint64_t u) {
    uint64_t v = u * 3935559000370003845ul + 2691343689449507681ul;
    v ^= v >> 21;
    v ^= v << 37;
    v ^= v >> 4;
    v *= 4768777513237032717ul;
    v ^= v << 20;
    v ^= v >> 41;
    v ^= v << 5;
    return v;
}

struct random_generator {
 public:
  using result_type = size_t;
  explicit random_generator(size_t seed) : state(seed) { }
  random_generator() : state(0) { }
  void seed(result_type value = 0) { state = value; }
  result_type operator()() { return state = hash64(state); }
  static constexpr result_type max() { return std::numeric_limits<result_type>::max(); }
  static constexpr result_type min() { return std::numeric_limits<result_type>::lowest(); }
  random_generator operator[](size_t i) const {
    return random_generator(static_cast<size_t>(hash64((i+1)*0x7fffffff + state))); }
 private:
  result_type state = 0;
};

struct random {
 public:
  random(size_t seed) : state(seed){}
  random() : state(0){}
  random fork(uint64_t i) const { return random(static_cast<size_t>(hash64(hash64(i + state)))); }
  random next() const { return fork(0); }
  size_t ith_rand(uint64_t i) const { return static_cast<size_t>(hash64(i + state)); }
  size_t operator[](size_t i) const { return ith_rand(i); }
  size_t rand() { return ith_rand(++state); }
  size_t max() { return std::numeric_limits<size_t>::max(); }
 private:
  size_t state = 0;
};

}

#endif