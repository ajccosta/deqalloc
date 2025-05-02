/*
* Copyright 2020
*   Andreia Correia <andreia.veiga@unine.ch>
*   Pedro Ramalhete <pramalhe@gmail.com>
*   Pascal Felber <pascal.felber@unine.ch>
*
* This work is published under the MIT license.
*/

#ifndef HARRIS_LINKED_LIST_H
#define HARRIS_LINKED_LIST_H

#include <atomic>
#include <functional>

/*
 * This is a modified Harris Linked List.
 * Original implementation by Andreia Correia, Predo Ramalhete and Pascal Felber.
 *
 * Modified version implemented by André Costa.
 *
 * This version is tailored to SegmentHeap's needs. This includes:
 *  - No keys or values are stored. We only care about the pointers themselves (they belong to a segment header).
 *  - Nodes are always inserted at the head. We want fresher segments to be faster to access.
 *  - The head sentinel node is not allocated from a node pool.
 *  - The tail sentinel node points to a statically allocated dummy node to save space.
 *  - Nodes are pre-allocated (they are part of the SegmentHeap header).
 *  - We assume the list represents a set because the user never inserts duplicates
 */

template <typename T = void*>
class HarrisLinkedList {
private:
    struct alignas(8) Node {
        std::atomic<Node*> next {nullptr};
        Node() = default;
    };

    static constexpr bool isMarked(Node* node) {
        return ((size_t) node & 0x1ULL);
    }

    static constexpr Node* getMarked(Node* node) {
        return (Node*)((size_t) node | 0x1ULL);
    }

    static constexpr Node* getUnmarked(Node* node) {
        return (Node*)((size_t) node & (~0x1ULL));
    }

    //Pointers to head and tail sentinel nodes of the list
    Node head;

    static inline Node dummy_tail {};
    static constexpr Node* tail = &dummy_tail;
     
public:
    HarrisLinkedList() {
        if(isMarked(tail)) {
            //Ensure that tail is not marked;
            // if tail is marked (e.g., 0x1), getUnmarked(tail)==tail will fail
            std::cerr<<"Static allocation of tail yielded marked pointer"<<std::endl;
            abort();
        }
        head.next = tail;
    }

    void add(T* new_node_) {
        Node* new_node = (Node*) new_node_;
        Node* left_node;
        do {
            //Always insert at the head
            left_node = &head;
            Node* right_node = left_node->next.load();
            new_node->next.store(right_node);
            if (left_node->next.compare_exchange_strong(right_node, new_node)) /*C2*/
                return;
        } while (true); /*B3*/
    }

    template <typename Thunk>
    bool remove(T* node_to_delete_, const Thunk& retire) {
        Node* node_to_delete = (Node*) node_to_delete_;
        Node* right_node;
        Node* right_node_next;
        Node* left_node;
        do {
            right_node = search(node_to_delete, left_node, retire);
            if ((right_node == tail) || (right_node != node_to_delete)) /*T1*/
                return false;
            right_node_next = right_node->next.load();
            if (!isMarked(right_node_next))
                if (right_node->next.compare_exchange_strong(right_node_next, getMarked(right_node_next))) break;
        } while (true); /*B4*/
        if (!left_node->next.compare_exchange_strong(right_node, right_node_next)) {/*C4*/
            right_node = search(right_node, left_node, retire);
        } else {
            retire((T)getUnmarked(right_node));
        }
        return true;
    }

    template <typename F>
    T* find(std::function<F> search_f) {
        //Node* node = head.next;
        //while(node != tail) {
        //  if(node->key >= key) break;
        //  node = getUnmarked(node->next);
        //} 
        //if ((node == tail) || (node->key != key))
        //  return nullptr;
        //else
        //  return node->value;
        return nullptr;
    }

    //Returns first element in list
    T peek() {
        Node* n = head.next;
        return (T)(n != tail ? n : nullptr);
    }

private:
    template <typename Thunk>
    Node* search(Node* search_node_, Node*& left_node, const Thunk& retire) {
        Node* search_node = static_cast<Node*>(search_node_);
        search_again:
        do {
            Node* left_node_next;
            Node* right_node = &head;
            Node* t_next = right_node->next.load(); /* 1: Find left_node and right_node */
            do {
                if (!isMarked(t_next)) {
                    left_node = right_node;
                    left_node_next = t_next;
                }
                right_node = getUnmarked(t_next);
                if (right_node == tail) break;
                t_next = right_node->next.load();
            } while (isMarked(t_next) || (right_node != search_node)); /*B1*/
            /* 2: Check nodes are adjacent */
            if (left_node_next == right_node)
                if ((right_node != tail) && isMarked(right_node->next.load())) goto search_again; /*G1*/
            else
                return right_node; /*R1*/
            /* 3: Remove one or more marked nodes */
            if (left_node->next.compare_exchange_strong(left_node_next, right_node)) /*C1*/  {
                Node* to_free = getUnmarked(left_node_next);
                while(to_free != right_node) {
                    retire((T)to_free);
                    to_free = getUnmarked(to_free->next);
                }
                if ((right_node != tail) && isMarked(right_node->next.load())) 
                    goto search_again; /*G2*/
                else
                    return right_node; /*R2*/
            }
        } while (true); /*B2*/
    }
};

#endif