// free-benchmark.c
// Micro-benchmark with 3 phases (repeatable runs):
//  (1) per-thread mallocs into partitioned shared array (partition size = n)
//  (2) main thread shuffles array (progress bar shown when verbose>=1)
//      - supports a target remote-percent (-p) that will cause shuffle to stop early
//      - shuffle maintains per-region remote counts incrementally (O(1) per swap)
//  (3) threads free pointers in their partition (measured)
//
// Verbosity:
//  - no -v: verbose==0 -> only print per-run throughput and final summary (includes remote %% in summary)
//  - -v    : verbose==1 -> show progress bars for phase 1 & 2 and minimal prints
//  - -vv   : verbose==2 -> show progress bars + detailed per-run info (per-thread remote %% etc.)
//
// New options:
//  -p PCT   target percentage of remote frees (0..100). Shuffle runs up to -s iterations but will
//           stop early when the remote %% in the array >= PCT.
//  -r RUNS  repeat the whole experiment RUNS times (default 1)
//
// Usage: ./free-benchmark -t THREADS -n PER_THREAD -s SHUFFLE_ITERS [-b ALLOC_SIZE] [-p TARGET_PCT] [-r RUNS] [-v]
// Examples:
//  ./free-benchmark -t 4 -n 100000 -s 1000000 -b 64        # no verbose, prints only throughput and summary
//  ./free-benchmark -t 4 -n 100000 -s 1000000 -b 64 -v     # show progress bars
//  ./free-benchmark -t 4 -n 100000 -s 1000000 -b 64 -vv    # show progress + detailed output
//  ./free-benchmark -t 4 -n 100000 -s 1000000 -b 64 -p 50  # stop shuffle early when 50%% remote achieved

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <stdatomic.h>

typedef struct {
    int id;
    size_t n;
    void **arr;
    int *owners;
    size_t alloc_size;
    pthread_barrier_t *bar_alloc_done;
    pthread_barrier_t *bar_shuffle_done;
    size_t remote_frees_precomputed; // set by main AFTER shuffle, BEFORE frees
} thread_arg_t;

/* Global progress counters (per-run) */
static atomic_ulong alloc_done = 0;   // allocations completed OR shuffle_total when in phase2
static atomic_ulong shuffle_done = 0; // shuffle iterations completed
static atomic_int phase = 0;          // 0=not started, 1=alloc, 2=shuffle, 3=freeing
static atomic_int progress_done = 0;  // 0=running, 1=stop progress thread

static inline double timespec_to_secs(const struct timespec *t){
    return t->tv_sec + t->tv_nsec * 1e-9;
}

void print_progress_bar(double fraction, int width, char *buf, size_t bufsz) {
    if (fraction < 0) fraction = 0;
    if (fraction > 1) fraction = 1;
    int filled = (int)(fraction * (double)width + 0.5);
    int pos = 0;
    pos += snprintf(buf + pos, bufsz - pos, "[");
    for (int i = 0; i < width; ++i) {
        if (i < filled) pos += snprintf(buf + pos, bufsz - pos, "=");
        else if (i == filled) pos += snprintf(buf + pos, bufsz - pos, ">");
        else pos += snprintf(buf + pos, bufsz - pos, " ");
    }
    pos += snprintf(buf + pos, bufsz - pos, "]");
}

/* Progress thread: updates progress bars for phase1 & phase2 */
void *progress_thread_fn(void *arg) {
    size_t total = (size_t)(uintptr_t)arg; // total items (t * n)
    const int bar_width = 40;
    struct timespec req = {0, 150 * 1000 * 1000}; // 150 ms
    while (atomic_load(&progress_done) == 0) {
        int cur_phase = atomic_load(&phase);
        if (cur_phase == 1) {
            unsigned long done = atomic_load(&alloc_done);
            double frac = (total == 0) ? 1.0 : (double)done / (double)total;
            char bar[256] = {0};
            print_progress_bar(frac, bar_width, bar, sizeof(bar));
            printf("\rPhase 1 (alloc): %s %6.2f%% (%lu / %zu)   ",bar, frac * 100.0, done, total);
            fflush(stdout);
        } else if (cur_phase == 2) {
            unsigned long done = atomic_load(&shuffle_done);
            unsigned long shuffle_total = atomic_load(&alloc_done); // main stores shuffle_total in alloc_done during phase2
            double frac = (shuffle_total == 0) ? 1.0 : (double)done / (double)shuffle_total;
            char bar[256] = {0};
            print_progress_bar(frac, bar_width, bar, sizeof(bar));
            printf("\rPhase 2 (shuffle): %s %6.2f%% (%lu / %lu)   ",bar, frac * 100.0, done, shuffle_total);
            fflush(stdout);
        } else if (cur_phase >= 3) {
            // Print both complete bars once and exit loop
            char bar1[256] = {0}, bar2[256] = {0};
            print_progress_bar(1.0, bar_width, bar1, sizeof(bar1));
            print_progress_bar(1.0, bar_width, bar2, sizeof(bar2));
            printf("\rPhase 1 (alloc): %s 100.00%% (%zu / %zu) | Phase 2 (shuffle): %s 100.00%%   ", bar1, total, total, bar2);
            fflush(stdout);
            break;
        } else {
            printf("\rWaiting to start...                                           ");
            fflush(stdout);
        }
        nanosleep(&req, NULL);
    }
    putchar('\n');
    return NULL;
}

void *worker_thread(void *arg0){
    thread_arg_t *a = (thread_arg_t*)arg0;
    size_t n = a->n;
    size_t base = (size_t)a->id * n;
    void **arr = a->arr;
    int *owners = a->owners;
    size_t alloc_size = a->alloc_size;

    // Phase 1: allocate and write pointer into region [base .. base+n)
    for(size_t i = 0; i < n; ++i){
        void *p = malloc(alloc_size);
        if (!p) {
            fprintf(stderr, "thread %d: malloc failed at i=%zu", a->id, i);
            exit(1);
        }
        // touch first byte to ensure allocation really occurs
        ((volatile unsigned char*)p)[0] = (unsigned char)a->id;
        arr[base + i] = p;
        owners[base + i] = a->id;

        // increment global allocation progress counter
        atomic_fetch_add(&alloc_done, 1UL);
    }

    // Announce done of phase 1 and wait for main to start shuffle
    int rc = pthread_barrier_wait(a->bar_alloc_done);
    if (rc != 0 && rc != PTHREAD_BARRIER_SERIAL_THREAD) {
        fprintf(stderr, "barrier wait error (alloc_done): %s", strerror(rc));
        exit(1);
    }

    // Wait for shuffle to finish (main will call the second barrier after shuffling)
    rc = pthread_barrier_wait(a->bar_shuffle_done);
    if (rc != 0 && rc != PTHREAD_BARRIER_SERIAL_THREAD) {
        fprintf(stderr, "barrier wait error (shuffle_done): %s", strerror(rc));
        exit(1);
    }

    // Phase 3: free pointers in our region (these pointers may have been moved there by shuffle)
    for(size_t i = 0; i < n; ++i){
        void *p = arr[base + i];
        if (p) free(p);
    }

    return NULL;
}

void usage(const char *prog){
    fprintf(stderr,
        "Usage: %s -t THREADS -n PER_THREAD -s SHUFFLE_ITERS [-b ALLOC_SIZE] [-p TARGET_PCT] [-r RUNS] [-v]"
        "  -t THREADS       number of worker threads"
        "  -n PER_THREAD    number of pointers each thread allocates (partition size)"
        "  -s SHUFFLE_ITERS number of random swap iterations performed by main thread (max)"
        "  -b ALLOC_SIZE    size (bytes) for each malloc (default 64)"
        "  -p TARGET_PCT    target percentage of remote frees (0..100). Shuffle stops early when reached"
        "  -r RUNS          number of repeated runs (default 1)"
        "  -v               increase verbosity: -v (progress bars), -vv (detailed output)", prog);
    exit(1);
}

int main(int argc, char **argv){
    int t = -1;
    long n = -1;
    long s = 0;
    long runs = 1;
    size_t alloc_size = 64;
    int verbose = 0; // 0=no, 1=progress, 2=more details
    double target_pct = -1.0; // if >=0, shuffle will try to reach this percent remote
    int opt;
    while ((opt = getopt(argc, argv, "t:n:s:b:p:r:v?h")) != -1) {
        switch(opt){
            case 't': t = atoi(optarg); break;
            case 'n': n = atol(optarg); break;
            case 's': s = atol(optarg); break;
            case 'b': alloc_size = (size_t)atol(optarg); break;
            case 'p': target_pct = atof(optarg); break;
            case 'r': runs = atol(optarg); break;
            case 'v': verbose++; break;
            case '?':
            case 'h':
            default: usage(argv[0]);
        }
    }
    if (t <= 0 || n <= 0) usage(argv[0]);
    if (s < 0 || runs <= 0) usage(argv[0]);
    if (target_pct >= 0.0 && (target_pct < 0.0 || target_pct > 100.0)) {
        fprintf(stderr, "target_pct must be between 0 and 100");
        return 1;
    }

    size_t total = (size_t)t * (size_t)n;

    if (verbose >= 1) {
        printf("Parameters: threads=%d per-thread=%ld total=%zu shuffle_iters(max)=%ld alloc_size=%zu runs=%ld target_pct=%s verbose=%d\n", \
               t, n, total, s, alloc_size, runs, \
               (target_pct < 0.0) ? "(none)" : "specified", \
               verbose);
    } else {
        printf("threads=%d per-thread=%ld shuffle_iters(max)=%ld alloc_size=%zu runs=%ld target_pct=%s\n", \
               t, n, s, alloc_size, runs, \
               (target_pct < 0.0) ? "(none)" : "specified");
    }

    double sum_ops_per_sec = 0.0;
    double sum_mb_per_sec = 0.0;
    double sum_remote_pct = 0.0;

    for(long run = 0; run <= runs; ++run) {
        if (verbose >= 1) printf("===== RUN %ld / %ld =====\n", run, runs);

        // allocate array of pointers and owners
        void **arr = malloc(sizeof(void*) * total);
        int *owners = malloc(sizeof(int) * total);
        if (!arr || !owners) {
            fprintf(stderr, "failed to allocate arrays of %zu entries\n", total);
            return 1;
        }
        for(size_t i = 0; i < total; ++i) { arr[i] = NULL; owners[i] = -1; }

        pthread_t *threads = malloc(sizeof(pthread_t) * t);
        thread_arg_t *targs = malloc(sizeof(thread_arg_t) * t);
        if (!threads || !targs) {
            fprintf(stderr, "out of memory\n");
            return 1;
        }

        pthread_barrier_t bar_alloc_done;
        pthread_barrier_t bar_shuffle_done;
        if (pthread_barrier_init(&bar_alloc_done, NULL, (unsigned)(t + 1)) != 0) {
            perror("pthread_barrier_init alloc_done\n");
            return 1;
        }
        if (pthread_barrier_init(&bar_shuffle_done, NULL, (unsigned)(t + 1)) != 0) {
            perror("pthread_barrier_init shuffle_done\n");
            return 1;
        }

        // reset globals for this run
        atomic_store(&alloc_done, 0UL);
        atomic_store(&shuffle_done, 0UL);
        atomic_store(&phase, 0);
        atomic_store(&progress_done, 0);

        // start progress thread (pass total as argument) only if verbose >= 1
        pthread_t progress_thread;
        int have_progress = 0;
        if (verbose >= 1) {
            have_progress = 1;
            int rc = pthread_create(&progress_thread, NULL, progress_thread_fn, (void*)(uintptr_t)total);
            if (rc) {
                fprintf(stderr, "pthread_create(progress) failed: %s", strerror(rc));
                return 1;
            }
        }

        // spawn worker threads
        for(int i = 0; i < t; ++i){
            targs[i].id = i;
            targs[i].n = (size_t)n;
            targs[i].arr = arr;
            targs[i].owners = owners;
            targs[i].alloc_size = alloc_size;
            targs[i].bar_alloc_done = &bar_alloc_done;
            targs[i].bar_shuffle_done = &bar_shuffle_done;
            targs[i].remote_frees_precomputed = 0;
            int rc2 = pthread_create(&threads[i], NULL, worker_thread, &targs[i]);
            if (rc2) {
                fprintf(stderr, "pthread_create(%d) failed: %s", i, strerror(rc2));
                return 1;
            }
        }

        // Set phase to 1 so progress thread shows allocation progress
        atomic_store(&phase, 1);

        // Wait for all workers to finish allocations
        if (verbose >= 1) printf("\nMain: waiting for workers to finish allocations (phase 1)...\n");
        int rcbar = pthread_barrier_wait(&bar_alloc_done);
        if (rcbar != 0 && rcbar != PTHREAD_BARRIER_SERIAL_THREAD) {
            fprintf(stderr, "barrier wait error (main alloc_done): %s", strerror(rcbar));
            return 1;
        }
        if (verbose >= 1) printf("\nMain: detected all allocations done. Beginning shuffle phase (phase 2).\n");

        // Phase 2: shuffle array in-place with up to s random swaps; shuffle owners too
        // We'll maintain per-region remote counts and a global total_remote count incrementally.
        // To let the progress thread know shuffle_total, store it in alloc_done temporarily.
        atomic_store(&alloc_done, (unsigned long)s); // progress thread reads this as shuffle_total during phase2
        atomic_store(&shuffle_done, 0UL);
        atomic_store(&phase, 2);

        // per-region remote counts
        size_t *remote_per_region = calloc((size_t)t, sizeof(size_t));
        if (!remote_per_region) { fprintf(stderr, "calloc remote_per_region failed"); return 1; }
        size_t total_remote = 0; // number of positions whose owner != region_id

        struct timespec ts_seed;
        clock_gettime(CLOCK_REALTIME, &ts_seed);
        unsigned int rnd = (unsigned int)(ts_seed.tv_nsec ^ ts_seed.tv_sec ^ (uintptr_t)arr);
        long performed_iters = 0;
        for(long iter = 0; iter < s; ++iter){
            size_t i = (size_t)(rand_r(&rnd) % total);
            size_t j = (size_t)(rand_r(&rnd) % total);
            if (i == j) {
                // still count it as an iteration
                performed_iters++;
                if ((iter & 63) == 0) atomic_fetch_add(&shuffle_done, 64UL);
                // check target pct
                if (target_pct >= 0.0) {
                    double cur_pct = 100.0 * ((double)total_remote / (double)total);
                    if (cur_pct >= target_pct) { performed_iters = iter + 1; break; }
                }
                continue;
            }
            size_t ri = i / (size_t)n;
            size_t rj = j / (size_t)n;
            int oi = owners[i];
            int oj = owners[j];

            // contributions before
            int ci_before = (oi != (int)ri);
            int cj_before = (oj != (int)rj);
            // contributions after swap
            int ci_after = (oj != (int)ri);
            int cj_after = (oi != (int)rj);

            // update per-region and total
            if (ri != rj) {
                remote_per_region[ri] += (ci_after - ci_before);
                remote_per_region[rj] += (cj_after - cj_before);
                total_remote += (ci_after - ci_before) + (cj_after - cj_before);
            }

            // swap owners
            owners[i] = oj;
            owners[j] = oi;

            performed_iters++;
            if ((iter & 63) == 0) atomic_fetch_add(&shuffle_done, 64UL);

            // if a target percent was requested, check and stop early when reached
            if (target_pct >= 0.0) {
                double cur_pct = 100.0 * ((double)total_remote / (double)total);
                //printf("Curr pct %lf\n", cur_pct);
                if (cur_pct >= target_pct) { performed_iters = iter + 1; break; }
            }
        }
        // ensure shuffle_done accurate
        atomic_store(&shuffle_done, (unsigned long)performed_iters);

        double achieved_pct = 100.0 * ((double)total_remote / (double)total);
        if (verbose >= 1) printf("\nMain: shuffle complete (performed %ld / %ld iterations). Achieved remote %% = %.2f\n", \
                                 performed_iters, s, achieved_pct);

        // Use the per-region remote counts as the precomputed remote-frees per thread
        for(int rid = 0; rid < t; ++rid) {
            targs[rid].remote_frees_precomputed = remote_per_region[rid];
        }

        if (verbose >= 2) {
            printf("Precomputed remote frees (counts per thread): total_remote=%zu achieved_pct=%.2f\n", \
                   total_remote, achieved_pct);
        }

        // Record start time for phase 3
        struct timespec tstart, tend;
        clock_gettime(CLOCK_MONOTONIC, &tstart);

        // Set phase to 3 so progress thread prints final bars
        atomic_store(&phase, 3);

        // Release workers to start freeing by waiting on second barrier (main waits; workers are blocked on it)
        int rc = pthread_barrier_wait(&bar_shuffle_done);
        if (rc != 0 && rc != PTHREAD_BARRIER_SERIAL_THREAD) {
            fprintf(stderr, "barrier wait error (main shuffle_done): %s", strerror(rc));
            return 1;
        }

        // Wait for all threads to finish frees
        for(int i = 0; i < t; ++i){
            pthread_join(threads[i], NULL);
        }

        clock_gettime(CLOCK_MONOTONIC, &tend);
        double secs = timespec_to_secs(&tend) - timespec_to_secs(&tstart);
        size_t total_ops = total; // total frees performed
        double ops_per_sec = total_ops / secs;
        double mb_freed = (double)total_ops * (double)alloc_size / (1024.0*1024.0);
        double mb_per_sec = mb_freed / secs;

        // report
        // Per-run: always print throughput (even verbose==0)
        printf("Run %ld: frees/sec=%.2f MiB/s=%.2f\n", run, ops_per_sec, mb_per_sec);

        if (verbose >= 2) {
            printf("Run %ld details:", run);
            printf("  total_ops=%zu elapsed=%.6f s frees/sec=%.2f MiB/s=%.2f alloc_size=%zu\n", \
                   total_ops, secs, ops_per_sec, mb_per_sec, alloc_size);
            printf("  precomputed remote frees total=%zu (%.2f%%)\n", total_remote, achieved_pct);
            printf("  per-thread remote %%:\n");
            for(int i = 0; i < t; ++i) {
                double pct = 100.0 * ((double)targs[i].remote_frees_precomputed / (double)targs[i].n);
                printf("    thread %d: %zu / %zu (%.2f%%)\n", targs[i].id, targs[i].remote_frees_precomputed, targs[i].n, pct);
            }
        }

        if(run > 0) { //ignore first run
          sum_ops_per_sec += ops_per_sec;
          sum_mb_per_sec += mb_per_sec;
          sum_remote_pct += achieved_pct;
        }

        // tell progress thread to stop and join it
        if (have_progress) {
            atomic_store(&progress_done, 1);
            pthread_join(progress_thread, NULL);
        }

        // cleanup for this run
        free(remote_per_region);
        pthread_barrier_destroy(&bar_alloc_done);
        pthread_barrier_destroy(&bar_shuffle_done);
        free(arr);
        free(owners);
        free(threads);
        free(targs);
    }

    if (runs > 1) {
        printf("===== SUMMARY across %ld runs =====\n", runs);
        printf("Average frees/sec: %.2f\n", sum_ops_per_sec / (double)runs);
        printf("Average MiB/s:     %.2f\n", sum_mb_per_sec / (double)runs);
        // As requested: summary prints percentage of remote frees (average across runs)
        printf("Average remote %% (across runs): %.2f%%\n", sum_remote_pct / (double)runs);
    } else {
        // if single run, still print remote %% in summary
        printf("Average remote %% (single run): %.2f%%\n", sum_remote_pct / (double)runs);
    }

    return 0;
}
