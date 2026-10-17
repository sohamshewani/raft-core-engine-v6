#ifndef RAFT_TYPES_H
#define RAFT_TYPES_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <pthread.h>

#define RAFT_CACHE_LINE_SIZE 64
#define RAFT_MAX_NODES 256

typedef enum {
    RAFT_ROLE_FOLLOWER = 0,
    RAFT_ROLE_CANDIDATE = 1,
    RAFT_ROLE_LEADER = 2
} RaftRole;

typedef struct {
    uint32_t term;
    uint32_t command_len;
    uint32_t crc32;
    uint32_t reserved;
    char payload[];
} __attribute__((aligned(8))) LogEntry;

typedef struct {
    uint64_t offset;
    uint32_t data_len;
    bool eof;
    uint32_t crc32;
    char data[];
} __attribute__((aligned(8))) SnapshotChunk;

typedef struct {
    uint32_t capacity;
    uint32_t head;
    uint32_t tail;
    size_t element_size;
    void *buffer;
    pthread_mutex_t lock;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} __attribute__((aligned(RAFT_CACHE_LINE_SIZE))) RingBuffer;

typedef struct {
    pthread_mutex_t mutex_log;
    pthread_mutex_t mutex_state;
    pthread_mutex_t mutex_transport;
    pthread_cond_t cond_append;
    pthread_cond_t cond_heartbeat;
} RaftLockHierarchy;

typedef struct {
    uint64_t current_term __attribute__((aligned(RAFT_CACHE_LINE_SIZE)));
    uint32_t voted_for;
    uint32_t node_id;
    RaftRole role;

    uint64_t commit_index __attribute__((aligned(RAFT_CACHE_LINE_SIZE)));
    uint64_t last_applied;

    uint64_t next_index[RAFT_MAX_NODES];
    uint64_t match_index[RAFT_MAX_NODES];

    LogEntry **log;
    uint64_t log_capacity;
    uint64_t log_count;

    RaftLockHierarchy locks;
    RingBuffer rpc_queue;
    
    bool is_running;
} __attribute__((aligned(RAFT_CACHE_LINE_SIZE))) RaftNode;

static inline void raft_mutex_lock_hierarchy(RaftLockHierarchy *l, RaftRole role) {
    pthread_mutex_lock(&l->mutex_log);
    pthread_mutex_lock(&l->mutex_state);
    pthread_mutex_lock(&l->mutex_transport);
}

static inline void raft_mutex_unlock_hierarchy(RaftLockHierarchy *l) {
    pthread_mutex_unlock(&l->mutex_transport);
    pthread_mutex_unlock(&l->mutex_state);
    pthread_mutex_unlock(&l->mutex_log);
}

#endif /* RAFT_TYPES_H */