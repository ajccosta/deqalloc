#ifndef TYPES_H
#define TYPES_H

#include <tuple>
#include <type_traits>
#include <utility>

//Detect if type is a tuple------------------------//
template<typename T>
struct is_tuple : std::false_type {};

template<typename... Args>
struct is_tuple<std::tuple<Args...>> : std::true_type {};

template<typename T>
constexpr bool is_tuple_v = is_tuple<T>::value;
//------------------------Detect if type is a tuple//

//Atomic Tuple-------------------------------------//
template <typename Tuple>
class atomic_tuple;

//Stores multiple atomic variables inside of a tuple.
//Each variable is loaded/stored individually, i.e.,
// not atomic in respect to each other.
template<typename... Args>
class atomic_tuple<std::tuple<Args...>> {
    private:
        static constexpr size_t n = sizeof...(Args);

        std::tuple<std::atomic<Args>...> vals;

    public:
        std::tuple<Args...> load(std::memory_order mo =
            std::memory_order_seq_cst) {
            return [&]<std::size_t... Is>(std::index_sequence<Is...>) {
                return std::make_tuple(std::get<Is>(vals).load(mo)...);
            }(std::index_sequence_for<Args...>{});
        }

        void store(std::tuple<Args...> new_vals,
            std::memory_order mo = std::memory_order_seq_cst) {
            [&]<std::size_t... Is>(std::index_sequence<Is...>) {
                (std::get<Is>(vals).store(std::get<Is>(new_vals), mo), ...);
            }(std::index_sequence_for<Args...>{});
        }
};
//-------------------------------------Atomic Tuple//

#endif