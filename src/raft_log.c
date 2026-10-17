#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

/* ============================================================================
 * CONSTANTS & MACROS
 * ============================================================================ */
#define RAFT_MAGIC_NUMBER       0x5241465457414C0ULL /* "RAFTWAL" */
#define RAFT_CACHE_LINE_SIZE    64
#define RAFT_MAX_PATH           1024
#define RAFT_INITIAL_LOG_CAP    1024

/* ============================================================================
 * DATA STRUCTURES (Matching System Spec)
 * ============================================================================ */

typedef struct __attribute__((aligned(RAFT_CACHE_LINE_SIZE))) {
    uint64_t term;
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
    uint32_t head;
    uint32_t tail;
    uint32_t capacity;
    uint32_t element_size;
    void *buffer;
} RingBuffer;

typedef struct __attribute__((aligned(RAFT_CACHE_LINE_SIZE))) {
    uint64_t current_term;
    uint32_t voted_for;
    uint64_t commit_index;
    uint64_t last_applied;
    
    // Log storage
    LogEntry **entries;
    uint64_t log_capacity;
    uint64_t log_count;
    uint64_t base_index; // Index of the first entry in entries (for compaction)

    // Leader state
    uint64_t *next_index;
    uint64_t *match_index;
    uint32_t num_peers;

    // Concurrency
    pthread_mutex_t mutex_log;
    pthread_mutex_t mutex_state;
    pthread_mutex_t mutex_transport;
    pthread_cond_t cond_append;
    pthread_cond_t cond_commit;

    // Persistence handle
    int wal_fd;
    char wal_path[RAFT_MAX_PATH];
} RaftNode;

/* WAL File Record Header */
typedef struct __attribute__((packed)) {
    uint64_t magic;
    uint64_t index;
    uint64_t term;
    uint32_t command_len;
    uint32_t crc32;
} WALRecordHeader;

/* ============================================================================
 * CRC32 IMPLEMENTATION
 * ============================================================================ */
static uint32_t raft_crc32(const uint8_t *data, size_t length) {
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < length; i++) {
        crc ^= data[i];
        for (int j = 0; j < 8; j++) {
            uint32_t mask = -(crc & 1);
            crc = (crc >> 1) ^ (0xEDB88320 & mask);
        }
    }
    return ~crc;
}

/* ============================================================================
 * RING BUFFER IMPLEMENTATION
 * ============================================================================ */
RingBuffer* raft_ring_buffer_init(uint32_t capacity, uint32_t element_size) {
    RingBuffer *rb = (RingBuffer*)malloc(sizeof(RingBuffer));
    if (!rb) return NULL;
    rb->head = 0;
    rb->tail = 0;
    rb->capacity = capacity;
    rb->element_size = element_size;
    rb->buffer = malloc(capacity * element_size);
    if (!rb->buffer) {
        free(rb);
        return NULL;
    }
    return rb;
}

void raft_ring_buffer_free(RingBuffer *rb) {
    if (rb) {
        free(rb->buffer);
        free(rb);
    }
}

bool raft_ring_buffer_push(RingBuffer *rb, const void *element) {
    uint32_t head = __atomic_load_n(&rb->head, __ATOMIC_RELAXED);
    uint32_t tail = __atomic_load_n(&rb->tail, __ATOMIC_ACQUIRE);
    
    if ((head + 1) % rb->capacity == tail) {
        return false; // Full
    }
    
    memcpy((char*)rb->buffer + (head * rb->element_size), element, rb->element_size);
    __atomic_store_n(&rb->head, (head + 1) % rb->capacity, __ATOMIC_RELEASE);
    return true;
}

bool raft_ring_buffer_pop(RingBuffer *rb, void *element) {
    uint32_t head = __atomic_load_n(&rb->head, __ATOMIC_ACQUIRE);
    uint32_t tail = __atomic_load_n(&rb->tail, __ATOMIC_RELAXED);
    
    if (tail == head) {
        return false; // Empty
    }
    
    memcpy(element, (char*)rb->buffer + (tail * rb->element_size), rb->element_size);
    __atomic_store_n(&rb->tail, (tail + 1) % rb->capacity, __ATOMIC_RELEASE);
    return true;
}

/* ============================================================================
 * FORWARD DECLARATIONS
 * ============================================================================ */
static int raft_wal_sync_entry(RaftNode *node, uint64_t index, LogEntry *entry);

/* ============================================================================
 * RAFT LOG MANAGEMENT & RECOVERY
 * ============================================================================ */

RaftNode* raft_node_init(const char *wal_path, uint32_t num_peers) {
    RaftNode *node = (RaftNode*)aligned_alloc(RAFT_CACHE_LINE_SIZE, sizeof(RaftNode));
    if (!node) return NULL;

    memset(node, 0, sizeof(RaftNode));
    node->current_term = 0;
    node->voted_for = 0;
    node->commit_index = 0;
    node->last_applied = 0;
    node->base_index = 1;
    node->num_peers = num_peers;

    snprintf(node->wal_path, sizeof(node->wal_path), "%s", wal_path);

    pthread_mutex_init(&node->mutex_log, NULL);
    pthread_mutex_init(&node->mutex_state, NULL);
    pthread_mutex_init(&node->mutex_transport, NULL);
    pthread_cond_init(&node->cond_append, NULL);
    pthread_cond_init(&node->cond_commit, NULL);

    node->log_capacity = RAFT_INITIAL_LOG_CAP;
    node->log_count = 0;
    node->entries = (LogEntry**)malloc(node->log_capacity * sizeof(LogEntry*));
    if (!node->entries) {
        free(node);
        return NULL;
    }

    if (num_peers > 0) {
        node->next_index = (uint64_t*)malloc(num_peers * sizeof(uint64_t));
        node->match_index = (uint64_t*)malloc(num_peers * sizeof(uint64_t));
        for (uint32_t i = 0; i < num_peers; i++) {
            node->next_index[i] = 1;
            node->match_index[i] = 0;
        }
    }

    node->wal_fd = open(node->wal_path, O_RDWR | O_CREAT | O_APPEND, 0644);
    if (node->wal_fd < 0) {
        free(node->entries);
        free(node);
        return NULL;
    }

    return node;
}

void raft_node_free(RaftNode *node) {
    if (!node) return;

    pthread_mutex_lock(&node->mutex_log);
    pthread_mutex_lock(&node->mutex_state);

    if (node->wal_fd >= 0) {
        close(node->wal_fd);
    }

    for (uint64_t i = 0; i < node->log_count; i++) {
        free(node->entries[i]);
    }
    free(node->entries);
    free(node->next_index);
    free(node->match_index);

    pthread_mutex_unlock(&node->mutex_state);
    pthread_mutex_unlock(&node->mutex_log);

    pthread_mutex_destroy(&node->mutex_log);
    pthread_mutex_destroy(&node->mutex_state);
    pthread_mutex_destroy(&node->mutex_transport);
    pthread_cond_destroy(&node->cond_append);
    pthread_cond_destroy(&node->cond_commit);

    free(node);
}

/* Crash Recovery: Rebuild state from WAL with strict CRC verification */
int raft_wal_recover(RaftNode *node) {
    pthread_mutex_lock(&node->mutex_log);
    pthread_mutex_lock(&node->mutex_state);

    // Reopen WAL in read-only mode to scan from beginning
    int recover_fd = open(node->wal_path, O_RDONLY);
    if (recover_fd < 0) {
        pthread_mutex_unlock(&node->mutex_state);
        pthread_mutex_unlock(&node->mutex_log);
        return (errno == ENOENT) ? 0 : -1; // ENOENT is fine for new nodes
    }

    off_t valid_eof = 0;
    
    while (true) {
        WALRecordHeader header;
        ssize_t n = read(recover_fd, &header, sizeof(WALRecordHeader));
        if (n == 0) {
            break; // EOF reached cleanly
        }
        if (n < (ssize_t)sizeof(WALRecordHeader)) {
            // Partial header read -> corruption / crash truncation point
            break;
        }

        if (header.magic != RAFT_MAGIC_NUMBER) {
            // Magic mismatch -> corruption detected
            break;
        }

        char *payload = malloc(header.command_len);
        if (!payload) {
            close(recover_fd);
            pthread_mutex_unlock(&node->mutex_state);
            pthread_mutex_unlock(&node->mutex_log);
            return -ENOMEM;
        }

        ssize_t payload_read = read(recover_fd, payload, header.command_len);
        if (payload_read != (ssize_t)header.command_len) {
            free(payload);
            break; // Truncated payload
        }

        uint32_t computed_crc = raft_crc32((uint8_t*)payload, header.command_len);
        if (computed_crc != header.crc32) {
            free(payload);
            break; // CRC validation failed -> truncation point
        }

        // Allocate and populate log entry
        LogEntry *entry = (LogEntry*)malloc(sizeof(LogEntry) + header.command_len);
        if (!entry) {
            free(payload);
            close(recover_fd);
            pthread_mutex_unlock(&node->mutex_state);
            pthread_mutex_unlock(&node->mutex_log);
            return -ENOMEM;
        }

        entry->term = header.term;
        entry->command_len = header.command_len;
        entry->crc32 = header.crc32;
        memcpy(entry->payload, payload, header.command_len);
        free(payload);

        // Resize log array if needed
        if (node->log_count >= node->log_capacity) {
            uint64_t new_cap = node->log_capacity * 2;
            LogEntry **new_entries = realloc(node->entries, new_cap * sizeof(LogEntry*));
            if (!new_entries) {
                free(entry);
                close(recover_fd);
                pthread_mutex_unlock(&node->mutex_state);
                pthread_mutex_unlock(&node->mutex_log);
                return -ENOMEM;
            }
            node->entries = new_entries;
            node->log_capacity = new_cap;
        }

        node->entries[node->log_count++] = entry;
        node->current_term = header.term > node->current_term ? header.term : node->current_term;

        // Valid checkpoint update
        valid_eof = lseek(recover_fd, 0, SEEK_CUR);
    }

    close(recover_fd);

    // Truncate WAL to eliminate trailing corrupted entries
    if (ftruncate(node->wal_fd, valid_eof) < 0) {
        pthread_mutex_unlock(&node->mutex_state);
        pthread_mutex_unlock(&node->mutex_log);
        return -1;
    }

    // Position WAL append pointer to the verified end
    lseek(node->wal_fd, 0, SEEK_END);

    node->last_applied = node->base_index + node->log_count - 1;
    if ((int64_t)node->last_applied < (int64_t)node->base_index) {
        node->last_applied = 0;
    }

    pthread_mutex_unlock(&node->mutex_state);
    pthread_mutex_unlock(&node->mutex_log);
    return 0;
}

static int raft_wal_sync_entry(RaftNode *node, uint64_t index, LogEntry *entry) {
    WALRecordHeader header;
    header.magic = RAFT_MAGIC_NUMBER;
    header.index = index;
    header.term = entry->term;
    header.command_len = entry->command_len;
    header.crc32 = entry->crc32;

    struct iovec iov[2];
    iov[0].iov_base = &header;
    iov[0].iov_len = sizeof(WALRecordHeader);
    iov[1].iov_base = entry->payload;
    iov[1].iov_len = entry->command_len;

    // Write header and payload atomically via writev
    ssize_t total_len = sizeof(WALRecordHeader) + entry->command_len;
    ssize_t written = writev(node->wal_fd, iov, 2);
    if (written != total_len) {
        return -EIO;
    }

    if (fsync(node->wal_fd) < 0) {
        return -EIO;
    }

    return 0;
}

/* Append entry maintaining invariant: Leader Append-Only / Log Matching */
int raft_log_append(RaftNode *node, uint64_t term, const void *command, uint32_t command_len) {
    pthread_mutex_lock(&node->mutex_log);

    LogEntry *entry = (LogEntry*)malloc(sizeof(LogEntry) + command_len);
    if (!entry) {
        pthread_mutex_unlock(&node->mutex_log);
        return -ENOMEM;
    }

    entry->term = term;
    entry->command_len = command_len;
    entry->crc32 = raft_crc32((const uint8_t*)command, command_len);
    memcpy(entry->payload, command, command_len);

    if (node->log_count >= node->log_capacity) {
        uint64_t new_cap = node->log_capacity * 2;
        LogEntry **new_entries = realloc(node->entries, new_cap * sizeof(LogEntry*));
        if (!new_entries) {
            free(entry);
            pthread_mutex_unlock(&node->mutex_log);
            return -ENOMEM;
        }
        node->entries = new_entries;
        node->log_capacity = new_cap;
    }

    uint64_t absolute_index = node->base_index + node->log_count;
    
    // Write-Ahead Logging persistence sync
    if (raft_wal_sync_entry(node,