#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <sys/time.h>
#include <math.h>

#ifdef RAFT_CORE_ENGINE_INCLUDE
#include RAFT_CORE_ENGINE_INCLUDE
#else
// Mock implementation if engine headers are not provided externally at compile time.
// This allows the microbenchmark to be self-contained and verifiable.
typedef struct { uint64_t term; uint64_t index; } raft_entry_t;
typedef void* raft_engine_t;

raft_engine_t* raft_engine_open(const char* path) { (void)path; return (raft_engine_t*)0x1; }
void raft_engine_close(raft_engine_t* e) { (void)e; }
int raft_engine_append(raft_engine_t* e, const raft_entry_t* entry) { (void)e; (void)entry; return 0; }
#endif

#define WARMUP_ITERATIONS 10000
#define MEASURE_ITERATIONS 100000
#define MAX_SAMPLES MEASURE_ITERATIONS

typedef struct {
    uint64_t *latencies;
    size_t count;
    size_t capacity;
} latency_stats_t;

static inline uint64_t get_nanos(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static int compare_uint64(const void *a, const void *b) {
    uint64_t fa = *(const uint64_t *)a;
    uint64_t fb = *(const uint64_t *)b;
    return (fa > fb) - (fa < fb);
}

int main(int argc, char **argv) {
    (void)argc;
    (void)argv;

    const char *db_path = "./raft_bench_data";
    
    // Initialize Raft Core Engine
    raft_engine_t *engine = raft_engine_open(db_path);
    if (!engine) {
        fprintf(stderr, "Failed to open raft core engine\n");
        return 1;
    }

    uint64_t *latencies = (uint64_t *)malloc(sizeof(uint64_t) * MEASURE_ITERATIONS);
    if (!latencies) {
        fprintf(stderr, "Memory allocation failed\n");
        raft_engine_close(engine);
        return 1;
    }

    raft_entry_t entry;
    entry.term = 1;
    entry.index = 0;
    
    char payload[256];
    memset(payload, 'x', sizeof(payload));

    // Warmup phase
    for (uint64_t i = 0; i < WARMUP_ITERATIONS; i++) {
        entry.index = i;
        if (raft_engine_append(engine, &entry) != 0) {
            fprintf(stderr, "Warmup append failed at iteration %lu\n", i);
        }
    }

    // Measurement phase
    uint64_t start_time = get_nanos();
    
    for (size_t i = 0; i < MEASURE_ITERATIONS; i++) {
        entry.index = WARMUP_ITERATIONS + i;
        
        uint64_t op_start = get_nanos();
        int rc = raft_engine_append(engine, &entry);
        uint64_t op_end = get_nanos();

        if (rc != 0) {
            fprintf(stderr, "Measurement append failed at iteration %zu\n", i);
        }
        
        latencies[i] = op_end - op_start;
    }

    uint64_t end_time = get_nanos();
    uint64_t total_duration_ns = end_time - start_time;

    // Compute metrics
    qsort(latencies, MEASURE_ITERATIONS, sizeof(uint64_t), compare_uint64);

    uint64_t p50 = latencies[(size_t)(MEASURE_ITERATIONS * 0.50)];
    uint64_t p90 = latencies[(size_t)(MEASURE_ITERATIONS * 0.90)];
    uint64_t p99 = latencies[(size_t)(MEASURE_ITERATIONS * 0.99)];
    uint64_t p999 = latencies[(size_t)(MEASURE_ITERATIONS * 0.999)];

    double duration_sec = (double)total_duration_ns / 1e9;
    double ops_sec = (double)MEASURE_ITERATIONS / duration_sec;

    // Output formatted tabular metrics to stdout
    printf("\n");
    printf("+------------------------------------------------------------+\n");
    printf("|           RAFT CORE ENGINE - THROUGHPUT & LATENCY          |\n");
    printf("+--------------------------+---------------------------------+\n");
    printf("| Metric                   | Value                           |\n");
    printf("+--------------------------+---------------------------------+\n");
    printf("| Total Operations         | %-31lu |\n", (unsigned long)MEASURE_ITERATIONS);
    printf("| Duration (sec)           | %-31.4f |\n", duration_sec);
    printf("| Throughput (ops/sec)     | %-31.2f |\n", ops_sec);
    printf("+--------------------------+---------------------------------+\n");
    printf("| Latency p50              | %-27.2f us |\n", (double)p50 / 1000.0);
    printf("| Latency p90              | %-27.2f us |\n", (double)p90 / 1000.0);
    printf("| Latency p99              | %-27.2f us |\n", (double)p99 / 1000.0);
    printf("| Latency p99.9            | %-27.2f us |\n", (double)p999 / 1000.0);
    printf("+--------------------------+---------------------------------+\n");
    printf("\n");

    free(latencies);
    raft_engine_close(engine);

    return 0;
}