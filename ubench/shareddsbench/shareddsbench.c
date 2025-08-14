/* Written by ChatGPT
 * allocator_micro_benchmark.c
 *
 * Multi-threaded allocator micro-benchmark with per-thread local stats and
 * multiple buckets per thread implemented as simple lock-protected binary trees.
 *
 * Each thread allocates nodes of a configurable `node_size` (these are the
 * allocations measured). The owning thread inserts nodes into one of its
 * buckets (tree). Threads perform a mix of writes (alloc+insert) and removes
 * (pop+free) from their *own* buckets; there is no cross-thread stealing.
 *
 * Warmup: performs `warmup` mallocs first (storing pointers), then frees
 * those `warmup` pointers — not interleaved, to avoid reusing the same
 * addresses repeatedly.
 *
 * Latency: per-operation latencies are recorded into log2 bins (ns) and
 * summarized at the end with percentiles and an ASCII histogram.
 *
 * Build:
 *   gcc -O2 -pthread -std=gnu11 -o allocator_micro_benchmark allocator_micro_benchmark.c
 *
 * Usage (important options):
 *   -t <threads>       threads (default 4)
 *   -n <count>         operations per thread (default 100000)
 *   -s <node_size>     size in bytes of each node allocation (default 64)
 *   -m <buckets>       buckets per thread (default 4)
 *   -z <write_percent> percentage (0-100) of iterations that are writes
 *                      (alloc+insert). The remainder are removes (pop+free).
 *                      Default 100 (all writes).
 *   -w <warmup>        warmup mallocs per thread (allocate then free) (default 1000)
 *   -a                 pin threads to CPUs (round-robin)
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <stdatomic.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sched.h>
#include <getopt.h>

#define LAT_BINS 64
#define HIST_ASCII_WIDTH 50
#define RESERVOIR_SAMPLES 0 // not used; we use histogram bins only

typedef struct tree_node {
    struct tree_node *left;
    struct tree_node *right;
    /* no payload accesses — allocation size is sizeof(tree_node)+node_size */
} tree_node_t;

typedef struct bucket {
    pthread_mutex_t lock;
    tree_node_t *root;
} bucket_t;

struct thr_args {
    int id;
    uint64_t count;
    size_t node_size;
    uint64_t warmup;
    bool pin;
    unsigned int rand_seed;
    int buckets_per_thread;
    // outputs
    uint64_t mallocs_during;
    uint64_t frees_during;
    uint64_t insert_steps_during;
    uint64_t remove_steps_during;
    uint64_t insert_hist[LAT_BINS];
    uint64_t remove_hist[LAT_BINS];
    uint64_t insert_ops;
    uint64_t remove_ops;
};

// Global buckets: threads * buckets_per_thread
static bucket_t *buckets = NULL;
static int g_threads = 0;
static int g_buckets_per_thread = 4;

// Start synchronisation
static atomic_int ready_count = 0;
static atomic_bool start_flag = false;

// Globals controlled from main
int write_percent_glob = 100; // -z

static inline double timespec_to_sec(const struct timespec *t) {
    return (double)t->tv_sec + (double)t->tv_nsec / 1e9;
}

static inline uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

// record latency into log2 histogram bins (ns)
static inline void record_latency(uint64_t *hist, uint64_t lat_ns) {
    int idx;
    if (lat_ns == 0) idx = 0;
    else idx = 63 - __builtin_clzll(lat_ns);
    if (idx < 0) idx = 0;
    if (idx >= LAT_BINS) idx = LAT_BINS - 1;
    hist[idx]++;
}

// Insert a node into the bucket's binary tree. Called by the owner thread.
// 'steps_out' is incremented by the number of traversal steps performed.
static void insert_bucket(int bucket, tree_node_t *n, unsigned int *seed, uint64_t *steps_out) {
    bucket_t *b = &buckets[bucket];
    pthread_mutex_lock(&b->lock);
    if (b->root == NULL) {
        n->left = n->right = NULL;
        b->root = n;
        // zero traversal steps when tree empty
    } else {
        tree_node_t *cur = b->root;
        // count inspecting root
        if (steps_out) (*steps_out)++;
        while (true) {
            int r = rand_r(seed) & 1;
            if (r == 0) {
                if (cur->left) {
                    cur = cur->left;
                    if (steps_out) (*steps_out)++;
                } else {
                    cur->left = n; n->left = n->right = NULL; break;
                }
            } else {
                if (cur->right) {
                    cur = cur->right;
                    if (steps_out) (*steps_out)++;
                } else {
                    cur->right = n; n->left = n->right = NULL; break;
                }
            }
        }
    }
    pthread_mutex_unlock(&b->lock);
}

// Pop (remove) the root node from the bucket's tree and return it. Returns NULL if empty.
// 'steps_out' is incremented by traversal steps performed while removing.
static tree_node_t *pop_bucket(int bucket, uint64_t *steps_out) {
    bucket_t *b = &buckets[bucket];
    pthread_mutex_lock(&b->lock);
    tree_node_t *root = b->root;
    if (root == NULL) {
        pthread_mutex_unlock(&b->lock);
        return NULL;
    }

    if (steps_out) (*steps_out)++; // inspected root

    if (root->left == NULL) {
        b->root = root->right;
    } else {
        // find rightmost node in left subtree
        tree_node_t *parent = root;
        tree_node_t *rightmost = root->left;
        if (steps_out) (*steps_out)++; // inspected rightmost (first)
        while (rightmost->right) {
            parent = rightmost;
            rightmost = rightmost->right;
            if (steps_out) (*steps_out)++;
        }
        // detach rightmost from its parent
        if (parent == root) {
            // root->left had no right child
            parent->left = rightmost->left;
        } else {
            parent->right = rightmost->left;
        }
        // replace root with rightmost
        rightmost->left = root->left;
        rightmost->right = root->right;
        b->root = rightmost;
    }

    // detach children from removed node to make it a standalone node
    root->left = root->right = NULL;
    pthread_mutex_unlock(&b->lock);
    return root;
}

// Helper to recursively free a tree (used when draining at the end). Uses an explicit stack.
static void drain_and_free_tree(tree_node_t *root) {
    if (!root) return;
    // use a manual stack to avoid recursion
    tree_node_t **stack = malloc(sizeof(tree_node_t *) * 64);
    size_t cap = 64;
    size_t top = 0;
    stack[top++] = root;
    while (top) {
        tree_node_t *n = stack[--top];
        if (n->left) {
            if (top == cap) {
                cap *= 2;
                stack = realloc(stack, sizeof(tree_node_t *) * cap);
            }
            stack[top++] = n->left;
        }
        if (n->right) {
            if (top == cap) {
                cap *= 2;
                stack = realloc(stack, sizeof(tree_node_t *) * cap);
            }
            stack[top++] = n->right;
        }
        free(n);
    }
    free(stack);
}

static void *worker(void *arg) {
    struct thr_args *a = (struct thr_args *)arg;
    // local stats
    uint64_t local_mallocs = 0;
    uint64_t local_frees = 0;
    uint64_t local_insert_steps = 0;
    uint64_t local_remove_steps = 0;

    // local histograms
    memset(a->insert_hist, 0, sizeof(a->insert_hist));
    memset(a->remove_hist, 0, sizeof(a->remove_hist));
    a->insert_ops = 0;
    a->remove_ops = 0;

    // pin
    if (a->pin) {
#ifdef __linux__
        cpu_set_t cpus;
        CPU_ZERO(&cpus);
        int cpu = a->id % sysconf(_SC_NPROCESSORS_ONLN);
        CPU_SET(cpu, &cpus);
        pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpus);
#endif
    }

    unsigned int seed = a->rand_seed;

    // Warmup: allocate warmup pointers, then free them (not interleaved)
    if (a->warmup > 0) {
        void **warm = malloc(sizeof(void *) * a->warmup);
        if (!warm) { perror("warmup alloc array"); }
        else {
            for (uint64_t i = 0; i < a->warmup; ++i) {
                warm[i] = malloc(sizeof(tree_node_t) + a->node_size);
                if (!warm[i]) { perror("warmup malloc"); warm[i] = NULL; break; }
            }
            for (uint64_t i = 0; i < a->warmup; ++i) {
                if (warm[i]) free(warm[i]);
            }
            free(warm);
        }
    }

    // Signal ready and wait for start
    atomic_fetch_add_explicit(&ready_count, 1, memory_order_acq_rel);
    while (!atomic_load_explicit(&start_flag, memory_order_acquire)) {
        sched_yield();
    }

    // Timed region
    for (uint64_t i = 0; i < a->count; ++i) {
        int op_r = rand_r(&seed) % 100;
        if (op_r < write_percent_glob) {
            // write: allocate and insert into one of our buckets
            uint64_t t0 = now_ns();
            tree_node_t *n = malloc(sizeof(tree_node_t) + a->node_size);
            uint64_t t1 = now_ns();
            if (!n) { perror("malloc"); break; }
            // insert
            int local_bucket = a->id * a->buckets_per_thread + (rand_r(&seed) % a->buckets_per_thread);
            uint64_t before_steps = local_insert_steps;
            insert_bucket(local_bucket, n, &seed, &local_insert_steps);
            uint64_t t2 = now_ns();
            // record latencies: alloc latency = t1-t0 ; insert latency = t2-t1 ; combined = t2-t0
            record_latency(a->insert_hist, t2 - t0);
            a->insert_ops++;
            local_mallocs++;
        } else {
            // remove: pop from a bucket
            int local_bucket = rand_r(&seed) % (a->buckets_per_thread * g_threads);
            uint64_t t0 = now_ns();
            tree_node_t *st = pop_bucket(local_bucket, &local_remove_steps);
            uint64_t t1 = now_ns();
            if (st) {
                free(st);
                uint64_t t2 = now_ns();
                record_latency(a->remove_hist, t2 - t0);
                a->remove_ops++;
                local_frees++;
            }
        }
    }

    // store local stats in args for reduction by main thread
    a->mallocs_during = local_mallocs;
    a->frees_during = local_frees;
    a->insert_steps_during = local_insert_steps;
    a->remove_steps_during = local_remove_steps;
    return NULL;
}

// helper to merge histograms
static void merge_hist(uint64_t *dst, uint64_t *src) {
    for (int i = 0; i < LAT_BINS; ++i) dst[i] += src[i];
}

// compute percentile (approx) from log2 histogram
static uint64_t hist_percentile(uint64_t *hist, uint64_t total, double pct) {
    if (total == 0) return 0;
    uint64_t target = (uint64_t)((pct/100.0) * (double)total);
    if (target == 0) target = 1;
    uint64_t cum = 0;
    for (int i = 0; i < LAT_BINS; ++i) {
        cum += hist[i];
        if (cum >= target) {
            // return midpoint (ns) of bin i: [2^i, 2^(i+1)-1]
            uint64_t lo = (i == 0) ? 0ULL : (1ULL << i);
            uint64_t hi = (1ULL << (i+1)) - 1ULL;
            return (lo + hi) / 2ULL;
        }
    }
    // fallback
    return (1ULL << (LAT_BINS-1));
}

int main(int argc, char **argv) {
    int threads = 4;
    uint64_t count = 100000;
    size_t node_size = 64;
    uint64_t warmup = 1000;
    bool pin = false;
    int buckets_per_thread = 4;

    int opt;
    while ((opt = getopt(argc, argv, "t:n:s:m:z:w:ah")) != -1) {
        switch (opt) {
            case 't': threads = atoi(optarg); break;
            case 'n': count = strtoull(optarg, NULL, 10); break;
            case 's': node_size = (size_t)strtoull(optarg, NULL, 10); break;
            case 'm': buckets_per_thread = atoi(optarg); break;
            case 'z': write_percent_glob = atoi(optarg); break;
            case 'w': warmup = strtoull(optarg, NULL, 10); break;
            case 'a': pin = true; break;
            case 'h':
            default:
                fprintf(stderr, "Usage: %s [-t threads] [-n count_per_thread] [-s node_size] [-m buckets_per_thread] [-z write_percent] [-w warmup] [-a pin]\n", argv[0]);
                return 1;
        }
    }

    if (threads <= 0) threads = 1;
    if (buckets_per_thread <= 0) buckets_per_thread = 1;
    if (write_percent_glob < 0) write_percent_glob = 0;
    if (write_percent_glob > 100) write_percent_glob = 100;

    g_threads = threads;
    g_buckets_per_thread = buckets_per_thread;

    pthread_t *ths = malloc(sizeof(pthread_t) * threads);
    struct thr_args *args = malloc(sizeof(struct thr_args) * threads);
    if (!ths || !args) { perror("alloc threads"); return 1; }

    // allocate buckets
    size_t total_buckets = (size_t)threads * (size_t)buckets_per_thread;
    buckets = calloc(total_buckets, sizeof(bucket_t));
    if (!buckets) { perror("buckets"); return 1; }

    // init
    atomic_store(&ready_count, 0);
    atomic_store(&start_flag, false);

    for (int i = 0; i < threads; ++i) {
        for (int j = 0; j < buckets_per_thread; ++j) pthread_mutex_init(&buckets[i*buckets_per_thread + j].lock, NULL);

        args[i].id = i;
        args[i].count = count;
        args[i].node_size = node_size;
        args[i].warmup = warmup;
        args[i].pin = pin;
        args[i].rand_seed = (unsigned int)time(NULL) ^ (i * 997);
        args[i].buckets_per_thread = buckets_per_thread;
        args[i].mallocs_during = 0;
        args[i].frees_during = 0;
        args[i].insert_steps_during = 0;
        args[i].remove_steps_during = 0;
        memset(args[i].insert_hist, 0, sizeof(args[i].insert_hist));
        memset(args[i].remove_hist, 0, sizeof(args[i].remove_hist));
        args[i].insert_ops = 0;
        args[i].remove_ops = 0;

        if (pthread_create(&ths[i], NULL, worker, &args[i]) != 0) {
            perror("pthread_create"); return 1; }
    }

    // Wait for all threads to be ready
    while (atomic_load_explicit(&ready_count, memory_order_acquire) < threads) {
        sched_yield();
    }

    // Start timed region
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    atomic_store_explicit(&start_flag, true, memory_order_release);

    // Wait threads to finish
    for (int i = 0; i < threads; ++i) {
        pthread_join(ths[i], NULL);
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double elapsed = timespec_to_sec(&t1) - timespec_to_sec(&t0);

    // Reduce per-thread stats
    uint64_t total_mallocs = 0;
    uint64_t total_frees_during = 0;
    uint64_t total_insert_steps = 0;
    uint64_t total_remove_steps = 0;
    uint64_t total_insert_ops = 0;
    uint64_t total_remove_ops = 0;
    uint64_t insert_hist[LAT_BINS];
    uint64_t remove_hist[LAT_BINS];
    memset(insert_hist, 0, sizeof(insert_hist));
    memset(remove_hist, 0, sizeof(remove_hist));

    for (int i = 0; i < threads; ++i) {
        total_mallocs += args[i].mallocs_during;
        total_frees_during += args[i].frees_during;
        total_insert_steps += args[i].insert_steps_during;
        total_remove_steps += args[i].remove_steps_during;
        total_insert_ops += args[i].insert_ops;
        total_remove_ops += args[i].remove_ops;
        merge_hist(insert_hist, args[i].insert_hist);
        merge_hist(remove_hist, args[i].remove_hist);
    }

    uint64_t total_ops_during = total_insert_ops + total_remove_ops;
    double ops_per_sec = (double)total_ops_during / elapsed;

    printf("threads=%d count_per_thread=%llu node_size=%zu buckets_per_thread=%d write_percent=%d warmup=%llu\n", \
           threads, (unsigned long long)count, node_size, buckets_per_thread, write_percent_glob, (unsigned long long)warmup);
    printf("measured_elapsed=%.6f s\n", elapsed);
    printf("total_inserts=%llu total_removes=%llu total_ops_during=%llu throughput=%.0f ops/s\n", \
           (unsigned long long)total_insert_ops, \
           (unsigned long long)total_remove_ops, \
           (unsigned long long)total_ops_during, \
           ops_per_sec);
    printf("total_insert_steps=%llu total_remove_steps=%llu\n", \
           (unsigned long long)total_insert_steps, \
           (unsigned long long)total_remove_steps);

    // Latency summaries
    printf("Insert latency summary (ns):\n");
    printf("  samples=%llu\n", (unsigned long long)total_insert_ops);
    if (total_insert_ops) {
        printf("  p50=%lluns p90=%lluns p99=%lluns p99.9=%lluns\n", \
               (unsigned long long)hist_percentile(insert_hist, total_insert_ops, 50.0), \
               (unsigned long long)hist_percentile(insert_hist, total_insert_ops, 90.0), \
               (unsigned long long)hist_percentile(insert_hist, total_insert_ops, 99.0), \
               (unsigned long long)hist_percentile(insert_hist, total_insert_ops, 99.9));
    }

    printf("Remove latency summary (ns):\n");
    printf("  samples=%llu\n", (unsigned long long)total_remove_ops);
    if (total_remove_ops) {
        printf("  p50=%lluns p90=%lluns p99=%lluns p99.9=%lluns\n", \
               (unsigned long long)hist_percentile(remove_hist, total_remove_ops, 50.0), \
               (unsigned long long)hist_percentile(remove_hist, total_remove_ops, 90.0), \
               (unsigned long long)hist_percentile(remove_hist, total_remove_ops, 99.0), \
               (unsigned long long)hist_percentile(remove_hist, total_remove_ops, 99.9));
    }

    // Drain remaining nodes from all buckets and free (not counted)
    for (int b = 0; b < (int)total_buckets; ++b) {
        pthread_mutex_lock(&buckets[b].lock);
        tree_node_t *root = buckets[b].root;
        buckets[b].root = NULL;
        pthread_mutex_unlock(&buckets[b].lock);
        drain_and_free_tree(root);
        pthread_mutex_destroy(&buckets[b].lock);
    }

    free(buckets);
    free(ths);
    free(args);
    return 0;
}
