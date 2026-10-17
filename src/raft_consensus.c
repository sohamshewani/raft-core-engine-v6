#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <unistd.h>
#include <errno.h>
#include <sys/time.h>

/* ============================================================================
 * CONSTANTS & MACROS
 * ============================================================================ */

#define RAFT_MAX_NODES 64
#define RAFT_ELECTION_TIMEOUT_MIN_MS 150
#define RAFT_ELECTION_TIMEOUT_MAX_MS 300
#define RAFT_HEARTBEAT_INTERVAL_MS 50

typedef enum {
    RAFT_ROLE_FOLLOWER = 0,
    RAFT_ROLE_CANDIDATE = 1,
    RAFT_ROLE_LEADER = 2
} RaftRole;

/* ============================================================================
 * DATA STRUCTURES (Matching System Spec)
 * ============================================================================ */

typedef struct __attribute__((aligned(64))) {
    uint32_t term;
    uint32_t command_len;
    uint32_t crc32;
    char payload[];
} LogEntry;

typedef struct {
    uint64_t offset;
    uint32_t data_len;
    bool eof;
    char data[];
} SnapshotChunk;

typedef struct {
    size_t head;
    size_t tail;
    size_t capacity;
    void **buffer;
    pthread_mutex_t lock;
} RingBuffer;

typedef struct __attribute__((aligned(64))) {
    uint32_t currentTerm;
    uint32_t votedFor;
    
    // Volatile state on all servers
    uint64_t commitIndex;
    uint64_t lastApplied;
    
    // Persistent log storage representation
    LogEntry **log;
    size_t log_capacity;
    size_t log_size;

    // Leader state
    uint64_t nextIndex[RAFT_MAX_NODES];
    uint64_t matchIndex[RAFT_MAX_NODES];

    RaftRole role;
    int node_id;
    int num_nodes;

    // Concurrency Primitives (Lock Hierarchy: Mutex_Log -> Mutex_State -> Mutex_Transport)
    pthread_mutex_t mutex_log;
    pthread_mutex_t mutex_state;
    pthread_mutex_t mutex_transport;

    pthread_cond_t cond_heartbeat;
    pthread_cond_t cond_append;

    bool running;
    pthread_t thread_election;
    pthread_t thread_heartbeat;
} RaftNode;

/* ============================================================================
 * UTILITY FUNCTIONS & CRC32
 * ============================================================================ */

static uint32_t calculate_crc32(const char *data, size_t length) {
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < length; i++) {
        crc ^= (uint8_t)data[i];
        for (int j = 0; j < 8; j++) {
            if (crc & 1) {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc = crc >> 1;
            }
        }
    }
    return ~crc;
}

static uint64_t get_current_time_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (tv.tv_sec * 1000ULL) + (tv.tv_usec / 1000ULL);
}

/* ============================================================================
 * RING BUFFER IMPLEMENTATION
 * ============================================================================ */

RingBuffer* ring_buffer_init(size_t capacity) {
    RingBuffer *rb = (RingBuffer *)malloc(sizeof(RingBuffer));
    if (!rb) return NULL;
    rb->capacity = capacity;
    rb->head = 0;
    rb->tail = 0;
    rb->buffer = (void **)malloc(sizeof(void *) * capacity);
    if (!rb->buffer) {
        free(rb);
        return NULL;
    }
    pthread_mutex_init(&rb->lock, NULL);
    return rb;
}

void ring_buffer_free(RingBuffer *rb) {
    if (!rb) return;
    pthread_mutex_destroy(&rb->lock);
    free(rb->buffer);
    free(rb);
}

bool ring_buffer_push(RingBuffer *rb, void *item) {
    pthread_mutex_lock(&rb->lock);
    size_t next_tail = (rb->tail + 1) % rb->capacity;
    if (next_tail == rb->head) {
        pthread_mutex_unlock(&rb->lock);
        return false; // Full
    }
    rb->buffer[rb->tail] = item;
    rb->tail = next_tail;
    pthread_mutex_unlock(&rb->lock);
    return true;
}

void* ring_buffer_pop(RingBuffer *rb) {
    pthread_mutex_lock(&rb->lock);
    if (rb->head == rb->tail) {
        pthread_mutex_unlock(&rb->lock);
        return NULL; // Empty
    }
    void *item = rb->buffer[rb->head];
    rb->head = (rb->head + 1) % rb->capacity;
    pthread_mutex_unlock(&rb->lock);
    return item;
}

/* ============================================================================
 * RAFT NODE LIFECYCLE & LOCK HIERARCHY MANAGEMENT
 * ============================================================================ */

static inline void raft_lock_hierarchy_acquire(RaftNode *node) {
    pthread_mutex_lock(&node->mutex_log);
    pthread_mutex_lock(&node->mutex_state);
    pthread_mutex_lock(&node->mutex_transport);
}

static inline void raft_lock_hierarchy_release(RaftNode *node) {
    pthread_mutex_unlock(&node->mutex_transport);
    pthread_mutex_unlock(&node->mutex_state);
    pthread_mutex_unlock(&node->mutex_log);
}

RaftNode* raft_node_init(int node_id, int num_nodes) {
    RaftNode *node = (RaftNode *)malloc(sizeof(RaftNode));
    if (!node) return NULL;

    node->node_id = node_id;
    node->num_nodes = num_nodes;
    node->currentTerm = 0;
    node->votedFor = -1;
    node->commitIndex = 0;
    node->lastApplied = 0;
    node->role = RAFT_ROLE_FOLLOWER;
    node->running = true;

    node->log_capacity = 1024;
    node->log_size = 0;
    node->log = (LogEntry **)malloc(sizeof(LogEntry *) * node->log_capacity);
    if (!node->log) {
        free(node);
        return NULL;
    }

    for (int i = 0; i < RAFT_MAX_NODES; i++) {
        node->nextIndex[i] = 1;
        node->matchIndex[i] = 0;
    }

    pthread_mutex_init(&node->mutex_log, NULL);
    pthread_mutex_init(&node->mutex_state, NULL);
    pthread_mutex_init(&node->mutex_transport, NULL);

    pthread_cond_init(&node->cond_heartbeat, NULL);
    pthread_cond_init(&node->cond_append, NULL);

    return node;
}

void raft_node_free(RaftNode *node) {
    if (!node) return;

    raft_lock_hierarchy_acquire(node);
    node->running = false;
    pthread_cond_broadcast(&node->cond_heartbeat);
    pthread_cond_broadcast(&node->cond_append);
    raft_lock_hierarchy_release(node);

    pthread_join(node->thread_election, NULL);
    pthread_join(node->thread_heartbeat, NULL);

    pthread_mutex_lock(&node->mutex_log);
    for (size_t i = 0; i < node->log_size; i++) {
        free(node->log[i]);
    }
    free(node->log);
    pthread_mutex_unlock(&node->mutex_log);

    pthread_cond_destroy(&node->cond_append);
    pthread_cond_destroy(&node->cond_heartbeat);

    pthread_mutex_destroy(&node->mutex_transport);
    pthread_mutex_destroy(&node->mutex_state);
    pthread_mutex_destroy(&node->mutex_log);

    free(node);
}

/* ============================================================================
 * LOG & STATE MACHINE OPERATIONS
 * ============================================================================ */

bool raft_append_entry(RaftNode *node, const char *command, size_t len, uint32_t term) {
    pthread_mutex_lock(&node->mutex_log);

    if (node->log_size >= node->log_capacity) {
        size_t new_cap = node->log_capacity * 2;
        LogEntry **new_log = (LogEntry **)realloc(node->log, sizeof(LogEntry *) * new_cap);
        if (!new_log) {
            pthread_mutex_unlock(&node->mutex_log);
            return false;
        }
        node->log = new_log;
        node->log_capacity = new_cap;
    }

    LogEntry *entry = (LogEntry *)malloc(sizeof(LogEntry) + len);
    if (!entry) {
        pthread_mutex_unlock(&node->mutex_log);
        return false;
    }

    entry->term = term;
    entry->command_len = (uint32_t)len;
    memcpy(entry->payload, command, len);
    entry->crc32 = calculate_crc32(entry->payload, len);

    node->log[node->log_size++] = entry;

    pthread_mutex_unlock(&node->mutex_log);

    pthread_cond_signal(&node->cond_append);
    return true;
}

/* ============================================================================
 * RPC IMPLEMENTATIONS & SAFETY INVARIANTS
 * ============================================================================ */

typedef struct {
    uint32_t term;
    int candidateId;
    uint64_t lastLogIndex;
    uint32_t lastLogTerm;
} RequestVoteArgs;

typedef struct {
    uint32_t term;
    bool voteGranted;
} RequestVoteReply;

void raft_handle_request_vote(RaftNode *node, const RequestVoteArgs *args, RequestVoteReply *reply) {
    raft_lock_hierarchy_acquire(node);

    if (args->term > node->currentTerm) {
        __atomic_store_n(&node->currentTerm, args->term, __ATOMIC_SEQ_CST);
        node->role = RAFT_ROLE_FOLLOWER;
        node->votedFor = -1;
    }

    if (args->term == node->currentTerm && 
        (node->votedFor == -1 || node->votedFor == (uint32_t)args->candidateId)) {
        
        uint32_t last_log_term = 0;
        pthread_mutex_lock(&node->mutex_log);
        if (node->log_size > 0) {
            last_log_term = node->log[node->log_size - 1]->term;
        }
        uint64_t last_log_index = node->log_size;
        pthread_mutex_unlock(&node->mutex_log);

        bool log_up_to_date = (args->lastLogTerm > last_log_term) ||
                              (args->lastLogTerm == last_log_term && args->lastLogIndex >= last_log_index);

        if (log_up_to_date) {
            node->votedFor = args->candidateId;
            reply->voteGranted = true;
        } else {
            reply->voteGranted = false;
        }
    } else {
        reply->voteGranted = false;
    }

    reply->term = node->currentTerm;

    raft_lock_hierarchy_release(node);
}

typedef struct {
    uint32_t term;
    int leaderId;
    uint64_t prevLogIndex;
    uint32_t prevLogTerm;
    uint64_t leaderCommit;
    size_t entries_count;
    LogEntry **entries;
} AppendEntriesArgs;

typedef struct {
    uint32_t term;
    bool success;
    uint64_t matchIndex;
} AppendEntriesReply;

void raft_handle_append_entries(RaftNode *node, const AppendEntriesArgs *args, AppendEntriesReply *reply) {
    raft_lock_hierarchy_acquire(node);

    if (args->term > node->currentTerm) {
        __atomic_store_n(&node->currentTerm, args->term, __ATOMIC_SEQ_CST);
        node->role = RAFT_ROLE_FOLLOWER;
        node->votedFor = -1;
    }

    if (args->term < node->currentTerm) {
        reply->term = node->currentTerm;
        reply->success = false;
        reply->matchIndex = 0;
        raft_lock_hierarchy_release(node);
        return;
    }

    // Reset election timer on valid append entries from current leader
    node->role = RAFT_ROLE_FOLLOWER;

    pthread_mutex_lock(&node->mutex_log);
    
    // Log Matching Invariant check
    if (args->prevLogIndex > 0) {
        if (node->log_size < args->prevLogIndex || 
            node->log[args->prevLogIndex - 1]->term != args->prevLogTerm) {
            pthread_mutex_unlock(&node->mutex_log);
            reply->term = node->currentTerm;
            reply->success = false;
            reply->matchIndex = 0;
            raft_lock_hierarchy_release(node);
            return;
        }
    }

    // Overwrite conflicting entries or append new ones
    for (size_t i = 0; i < args->entries_count; i++) {
        uint64_t index = args->prevLogIndex + 1 + i;
        if (index <= node->log_size) {
            if (node->log[index - 1]->term != args->entries[i]->term) {
                // Truncate conflicting trailing entries
                for (size_t j = index - 1; j < node->log_size; j++) {
                    free(node->log[j]);
                }
                node->log_size = index - 1;
                // Append new entry
                node->log[node->log_size++] = args->entries[i];
            } else {
                // Entry already matches, free duplicate transport payload
                free(args->entries[i]);
            }
        } else {
            if (node->log_size >= node->log_capacity) {
                size_t new_cap = node->log_capacity * 2;
                LogEntry **new_log = (LogEntry **)realloc(node->log, sizeof(LogEntry *) * new_cap);
                if (new_log) {
                    node->log = new_log;
                    node->log_capacity = new_cap;
                }
            }
            node->log[node->log_size++] = args->entries[i];
        }
    }

    pthread_mutex_unlock(&node->mutex_log);

    if (args->leaderCommit > node->commitIndex) {
        pthread_mutex_lock(&node->mutex_log);
        node->commitIndex = (args->leaderCommit < node->log_size) ? args->leaderCommit : node->log_size;
        pthread_mutex_unlock(&node->mutex_log);
    }

    reply->term = node->currentTerm;
    reply->success = true;
    reply->matchIndex = node->log_size;

    raft_lock_hierarchy_release(node);
}

/* ============================================================================
 * BACKGROUND WORKER THREADS
 * ============================================================================ */

static void* raft_election_worker(void *arg) {
    RaftNode *node = (RaftNode *)arg;
    while (true) {
        pthread_mutex_lock(&node->mutex_state);
        if (!node->running) {
            pthread_mutex_unlock(&node