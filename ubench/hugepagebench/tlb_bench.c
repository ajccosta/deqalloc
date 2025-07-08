//This benchmark was almost entirely written by ChatGPT!
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <sys/mman.h>
#include <math.h>

#define HUGE_PAGE_SIZE (2 * 1024 * 1024)
#define ALLOC_SIZE     (2 * HUGE_PAGE_SIZE)  // 4MB
#define WORDS_PER_SEGMENT (HUGE_PAGE_SIZE / sizeof(uint64_t))

typedef enum {
    DIST_UNIFORM,
    DIST_ZIPFIAN
} dist_t;

atomic_bool start_bench = false;

typedef struct {
    uint64_t **segments;
    int num_segments;
    long iterations;
    unsigned int seed;
    dist_t dist;
    int thread_id;
    int num_threads;
    int *segment_indices; // precomputed
    int local_only;
    double zipf_norm;
    double zipf_s;
} thread_arg_t;

typedef struct {
    void *raw;        // full 4MB mapping
    uint64_t *aligned; // aligned 2MB region
} region_t;

double now_sec() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

int zipfian_index(unsigned int *r, int n, int mode, double norm, double s) {
    double z = (double)rand_r(r) / RAND_MAX;
    double acc = 0.0;
    for (int i = 1; i <= n; ++i) {
        acc += (1.0 / pow(i, s)) / norm;
        if (z <= acc) {
            int idx = (mode + i - 1) % n;
            return idx;
        }
    }
    return mode;
}

void *prepare_indices(void *arg) {
    thread_arg_t *targ = (thread_arg_t *)arg;
    int local_range = targ->local_only ? targ->num_segments / targ->num_threads : targ->num_segments;
    int range_start = targ->local_only ? targ->thread_id * local_range : 0;
    int range_end = targ->local_only ? range_start + local_range : targ->num_segments;

    unsigned int r = targ->seed;
    for (long j = 0; j < targ->iterations; ++j) {
        if (targ->dist == DIST_ZIPFIAN) {
            targ->segment_indices[j] = zipfian_index(&r, range_end - range_start, 0, targ->zipf_norm, targ->zipf_s) + range_start;
        } else {
            targ->segment_indices[j] = range_start + (rand_r(&r) % (range_end - range_start));
        }
    }
    return NULL;
}

void *thread_fn(void *arg) {
    thread_arg_t *targ = (thread_arg_t *)arg;
    volatile uint64_t sink = 0;
    while(!start_bench) {};
    for (long i = 0; i < targ->iterations; ++i) {
        int seg = targ->segment_indices[i];
        uint64_t *base = targ->segments[seg];
        int offset = rand_r(&targ->seed) % WORDS_PER_SEGMENT;
        sink += base[offset];
        sink += base[0];
        //sink += base[targ->thread_id * 8]; //each thread accesses a different cache line
    }
    return NULL;
}

int count_hugepages(region_t *regions, int num_segments) {
    FILE *fp = fopen("/proc/self/smaps", "r");
    if (!fp) {
        perror("fopen smaps");
        return -1;
    }

    int count = 0;
    char line[512];
    for (int i = 0; i < num_segments; ++i) {
        uintptr_t addr = (uintptr_t)regions[i].aligned;
        rewind(fp);
        while (fgets(line, sizeof(line), fp)) {
            uintptr_t start, end;
            if (sscanf(line, "%lx-%lx", &start, &end) != 2)
                continue;
            if (start <= addr && addr < end) {
                while (fgets(line, sizeof(line), fp)) {
                    if (strncmp(line, "AnonHugePages:", 14) == 0) {
                        long kb = 0;
                        sscanf(line + 14, "%ld", &kb);
                        if (kb >= 2048) count++;
                        break;
                    }
                    if (line[0] == '\n') break;
                }
                break;
            }
        }
    }
    fclose(fp);
    return count;
}

int main(int argc, char **argv) {
    int num_threads = 4;
    int num_segments = 64;
    long total_iterations = 1000000;
    unsigned int seed = time(NULL);
    int use_nohugepage = 0;
    int check_hugepages = 0;
    int local_only = 0;
    dist_t dist = DIST_UNIFORM;

    for (int i = 1; i < argc; ++i) {
        if (!strcmp(argv[i], "--threads") && i+1 < argc)
            num_threads = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--segments") && i+1 < argc)
            num_segments = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--iterations") && i+1 < argc)
            total_iterations = atol(argv[++i]);
        else if (!strcmp(argv[i], "--seed") && i+1 < argc)
            seed = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--nohugepage"))
            use_nohugepage = 1;
        else if (!strcmp(argv[i], "--check"))
            check_hugepages = 1;
        else if (!strcmp(argv[i], "--zipf"))
            dist = DIST_ZIPFIAN;
        else if (!strcmp(argv[i], "--local"))
            local_only = 1;
        else {
            fprintf(stderr, "Usage: %s [--threads N] [--segments N] [--iterations N] [--seed N] [--nohugepage] [--check] [--zipf] [--local]\n", argv[0]);
            return 1;
        }
    }

    if (local_only && num_threads > num_segments) {
        fprintf(stderr, "Error: Not enough segments (%d) for local-only mode with %d threads.\n", num_segments, num_threads);
        return 1;
    }

    long iterations_per_thread = total_iterations / num_threads;

    printf("Running with %d threads, %d segments, %ld total iterations (seed=%u), %s, dist=%s%s\n",
           num_threads, num_segments, total_iterations, seed,
           use_nohugepage ? "MADV_NOHUGEPAGE" : "MADV_HUGEPAGE",
           dist == DIST_ZIPFIAN ? "zipfian" : "uniform",
           local_only ? ", local-only" : "");

    region_t *regions = calloc(num_segments, sizeof(region_t));
    uint64_t **segments = calloc(num_segments, sizeof(uint64_t *));
    if (!regions || !segments) { perror("calloc"); return 1; }

    for (int i = 0; i < num_segments; ++i) {
        void *raw = mmap(NULL, ALLOC_SIZE, PROT_READ | PROT_WRITE,
                         MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (raw == MAP_FAILED) {
            perror("mmap");
            return 1;
        }

        uintptr_t aligned = ((uintptr_t)raw + HUGE_PAGE_SIZE - 1) & ~(HUGE_PAGE_SIZE - 1);
        regions[i].raw = raw;
        regions[i].aligned = (uint64_t *)aligned;
        segments[i] = regions[i].aligned;

        int adv = use_nohugepage ? MADV_NOHUGEPAGE : MADV_HUGEPAGE;
        if (madvise(segments[i], HUGE_PAGE_SIZE, adv) != 0) {
            perror("madvise");
        }
        for (size_t w = 0; w < WORDS_PER_SEGMENT; ++w) {
            segments[i][w] = 0;
        }
    }

    pthread_t *threads = calloc(num_threads, sizeof(pthread_t));
    thread_arg_t *args = calloc(num_threads, sizeof(thread_arg_t));

    double norm = 0.0, s = 1.1;
    if (dist == DIST_ZIPFIAN) {
        for (int i = 1; i <= num_segments; ++i)
            norm += 1.0 / pow(i, s);
    }

    pthread_t *prep_threads = calloc(num_threads, sizeof(pthread_t));

    for (int i = 0; i < num_threads; ++i) {
        args[i].segments = segments;
        args[i].num_segments = num_segments;
        args[i].iterations = iterations_per_thread;
        args[i].seed = seed + i;
        args[i].dist = dist;
        args[i].thread_id = i;
        args[i].num_threads = num_threads;
        args[i].segment_indices = malloc(sizeof(int) * iterations_per_thread);
        args[i].local_only = local_only;
        args[i].zipf_norm = norm;
        args[i].zipf_s = s;
        pthread_create(&prep_threads[i], NULL, prepare_indices, &args[i]);
    }

    for (int i = 0; i < num_threads; ++i)
        pthread_join(prep_threads[i], NULL);

    free(prep_threads);

    usleep(10000);

    double start = now_sec();
    for (int i = 0; i < num_threads; ++i) {
        pthread_create(&threads[i], NULL, thread_fn, &args[i]);
    }

    start_bench = true;

    for (int i = 0; i < num_threads; ++i)
        pthread_join(threads[i], NULL);
    double end = now_sec();

    printf("Time: %.3f sec\n", end - start);
    printf("Throughput: %.2f M accesses/sec\n", total_iterations / (1e6 * (end - start)));

    if (check_hugepages) {
        int huge_used = count_hugepages(regions, num_segments);
        if (huge_used >= 0)
            printf("HugePages in use: %d / %d segments\n", huge_used, num_segments);
    }

    return 0;
}