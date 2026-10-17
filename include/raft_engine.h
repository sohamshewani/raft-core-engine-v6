#ifndef RAFT_ENGINE_H
#define RAFT_ENGINE_H

#define _GNU_SOURCE
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <pthread.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

/* -------------------------------------------------------------------------
 * Constants & Limits
 * ------------------------------------------------------------------------- */
#define RAFT_CACHE_LINE_SIZE 64
#define RAFT_MAX_NODES       256
#define RAFT_MIN_ELECTION_MS 150
#define RAFT_MAX_ELECTION_MS 300

/* -------------------------------------------------------------------------
 * Data Structures (System Spec 1)
 * ------------------------------------------------------------------------- */

typedef enum {
    RAFT_ROLE_FOLLOWER = 0,
    RAFT_ROLE_CANDIDATE  = 1,
    RAFT_ROLE_LEADER     = 2,
    RAFT_ROLE_SHUTDOWN   = 3
} __attribute__((aligned(4))) RaftRole;

/* 
 * LogEntry: 64-bit aligned structure containing term, command length, 
 * CRC32 checksum, and flexible array member for payload.
 */
typedef struct __attribute__((aligned(8))) LogEntry {
    uint64_t term;
    uint32_t len;
    uint32_t crc32;
    uint8_t  payload[];
} LogEntry;

/*
 * SnapshotChunk: Stream-oriented transport frame featuring offset, 
 * data length, and EOF flags for incremental state transfer.
 */
typedef struct __attribute__((aligned(8))) SnapshotChunk {
    uint64_t term;
    uint64_t index;
    uint64_t offset;
    uint32_t data_len;
    bool     eof;
    uint8_t  data[];
} SnapshotChunk;

/*
 * RingBuffer: Lock-free, bounded circular queue optimized for single-producer,
 * multi-consumer RPC dispatching.
 */
typedef struct __attribute__((aligned(RAFT_CACHE_LINE_SIZE))) RingBuffer {
    uint8_t *buffer;
    size_t   capacity;
    size_t   element_size;
    volatile uint64_t head __attribute__((aligned(RAFT_CACHE_LINE_SIZE)));
    volatile uint64_t tail __attribute__((aligned(RAFT_CACHE_LINE_SIZE)));
} RingBuffer;

/*
 * RaftNode: Encapsulates persistent state, volatile state on all servers,
 * and leader state. Cache-line padding applied to prevent false sharing.
 */
typedef struct __attribute__((aligned(RAFT_CACHE_LINE_SIZE))) RaftNode {
    // Persistent state on all servers
    uint64_t current_term __attribute__((aligned(RAFT_CACHE_LINE_SIZE)));
    uint32_t voted_for    __attribute__((aligned(RAFT_CACHE_LINE_SIZE)));
    
    // Volatile state on all servers
    uint64_t commit_index __attribute__((aligned(RAFT_CACHE_LINE_SIZE)));
    uint64_t last_applied __attribute__((aligned(RAFT_CACHE_LINE_SIZE)));
    RaftRole role         __attribute__((aligned(4)));

    // Leader state (reallocated/initialized upon becoming leader)
    uint64_t next_index[RAFT_MAX_NODES];
    uint64_t match_index[RAFT_MAX_NODES];
    uint32_t num_nodes;
    uint32_t node_id;

    // Log storage metadata and arena reference
    int      wal_fd;
    char     wal_path[512];
    
    // Log entries management (in-memory index mapping to WAL offsets)
    LogEntry **log_cache;
    uint64_t   log_capacity;
    uint64_t   log_count;
    uint64_t   log_startIndex; // Absolute index of log_cache[0]

    // Concurrency Primitives (System Spec 2)
    // Lock hierarchy: Mutex_Log -> Mutex_State -> Mutex_Transport
    pthread_mutex_t mutex_log __attribute__((aligned(RAFT_CACHE_LINE_SIZE)));
    pthread_mutex_t mutex_state __attribute__((aligned(RAFT_CACHE_LINE_SIZE)));
    pthread_mutex_t mutex_transport __attribute__((aligned(RAFT_CACHE_LINE_SIZE)));

    pthread_cond_t  cond_append;
    pthread_cond_t  cond_heartbeat;
    pthread_t       background_thread;
    bool            running;

    // Arena memory allocator reference
    void *arena;
} RaftNode;

/* -------------------------------------------------------------------------
 * Arena Allocator Interface (System Spec 3)
 * ------------------------------------------------------------------------- */
typedef struct Arena Arena;

Arena* raft_arena_create(size_t block_size);
void*  raft_arena_alloc(Arena *arena, size_t size);
void   raft_arena_reset(Arena *arena);
void   raft_arena_destroy(Arena *arena);

/* -------------------------------------------------------------------------
 * Ring Buffer Interface (System Spec 1 - RingBuffer)
 * ------------------------------------------------------------------------- */
RingBuffer* raft_ring_buffer_init(size_t capacity, size_t element_size);
bool        raft_ring_buffer_push(RingBuffer *rb, const void *element);
bool        raft_ring_buffer_pop(RingBuffer *rb, void *element);
void        raft_ring_buffer_free(RingBuffer *rb);

/* -------------------------------------------------------------------------
 * Core Engine Lifecycle & Operations (Production Standards)
 * ------------------------------------------------------------------------- */

/**
 * Initializes the Raft node, executes crash recovery by reading the WAL,
 * verifying CRC32 checksums, truncating corruption, and reconstructing state.
 */
RaftNode* raft_node_init(uint32_t node_id, const char *wal_path, uint32_t num_nodes);

/**
 * Shuts down background workers, flushes buffers, and releases all resources cleanly.
 */
void raft_node_destroy(RaftNode *node);

/**
 * Submits a new client command to the Raft cluster.
 * If leader, appends to local log and dispatches AppendEntries.
 * If follower, returns error or leader hint.
 */
bool raft_node_submit(RaftNode *node, const uint8_t *command, uint32_t len, uint64_t *out_index);

/**
 * RPC Handlers
 */
typedef struct {
    uint64_t term;
    uint32_t candidate_id;
    uint64_t last_log_index;
    uint64_t last_log_term;
} RequestVoteArgs;

typedef struct {
    uint64_t term;
    bool     vote_granted;
} RequestVoteReply;

typedef struct {
    uint64_t term;
    uint32_t leader_id;
    uint64_t prev_log_index;
    uint64_t prev_log_term;
    uint64_t leader_commit;
    uint32_t entry_count;
    // Followed by serialized LogEntry stream
} AppendEntriesArgs;

typedef struct {
    uint64_t term;
    bool     success;
    uint64_t match_index;
} AppendEntriesReply;

void raft_handle_request_vote(RaftNode *node, const RequestVoteArgs *args, RequestVoteReply *reply);
void raft_handle_append_entries(RaftNode *node, const AppendEntriesArgs *args, const uint8_t *entries_payload, AppendEntriesReply *reply);
void raft_handle_install_snapshot(RaftNode *node, const SnapshotChunk *chunk, bool *success);

/**
 * Utility functions
 */
uint32_t raft_crc32(const uint8_t *data, size_t length);
void     raft_trigger_election(RaftNode *node);

#ifdef __cplusplus
}
#endif

#endif /* RAFT_ENGINE_H */