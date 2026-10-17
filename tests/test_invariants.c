#define _GNU_SOURCE
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <assert.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>

/* =========================================================================
 * RAFT CORE ENGINE IMPLEMENTATION (Embedded for self-contained compilation)
 * ========================================================================= */

typedef enum {
    RAFT_FOLLOWER = 0,
    RAFT_CANDIDATE,
    RAFT_LEADER
} raft_role_t;

typedef struct {
    uint64_t term;
    uint64_t index;
    uint32_t type; /* 0: normal, 1: config change */
    size_t   data_len;
    char     data[256];
} raft_entry_t;

typedef struct {
    uint64_t term;
    uint64_t candidate_id;
    uint64_t last_log_index;
    uint64_t last_log_term;
    bool     vote_granted;
} raft_msg_vote_t;

typedef struct {
    uint64_t term;
    uint64_t leader_id;
    uint64_t prev_log_index;
    uint64_t prev_log_term;
    uint64_t leader_commit;
    size_t   entries_count;
    raft_entry_t entries[16];
    bool     success;
    uint64_t match_index;
} raft_msg_append_t;

#define RAFT_MAX_LOGS 1024

typedef struct {
    uint64_t id;
    raft_role_t role;
    uint64_t current_term;
    uint64_t voted_for;
    
    raft_entry_t log[RAFT_MAX_LOGS];
    uint64_t log_startIndex; /* 1-based base index for truncated logs */
    size_t   log_count;      /* Number of entries currently in log */
    
    uint64_t commit_index;
    uint64_t last_applied;
    
    /* Leader state */
    uint64_t next_index[32];
    uint64_t match_index[32];
    
    pthread_rwlock_t lock;
} raft_node_t;

static void raft_node_init(raft_node_t *node, uint64_t id) {
    memset(node, 0, sizeof(raft_node_t));
    node->id = id;
    node->role = RAFT_FOLLOWER;
    node->current_term = 0;
    node->voted_for = 0;
    node->log_startIndex = 1;
    node->log_count = 0;
    node->commit_index = 0;
    node->last_applied = 0;
    pthread_rwlock_init(&node->lock, NULL);
}

static void raft_node_destroy(raft_node_t *node) {
    pthread_rwlock_destroy(&node->lock);
}

/* Internal helper: get log entry by 1-based absolute index */
static raft_entry_t* raft_get_log(raft_node_t *node, uint64_t index) {
    if (index < node->log_startIndex || index >= node->log_startIndex + node->log_count) {
        return NULL;
    }
    return &node->log[index - node->log_startIndex];
}

static uint64_t raft_last_log_index(raft_node_t *node) {
    if (node->log_count == 0) {
        return node->log_startIndex > 0 ? node->log_startIndex - 1 : 0;
    }
    return node->log_startIndex + node->log_count - 1;
}

static uint64_t raft_last_log_term(raft_node_t *node) {
    uint64_t idx = raft_last_log_index(node);
    if (idx < node->log_startIndex) {
        return 0;
    }
    raft_entry_t *entry = raft_get_log(node, idx);
    return entry ? entry->term : 0;
}

/* Step down invariant wrapper */
static void raft_become_follower(raft_node_t *node, uint64_t term) {
    node->role = RAFT_FOLLOWER;
    node->current_term = term;
    node->voted_for = 0;
}

/* RequestVote handler */
static bool raft_handle_request_vote(raft_node_t *node, const raft_msg_vote_t *req, raft_msg_vote_t *resp) {
    pthread_rwlock_wrlock(&node->lock);
    
    resp->candidate_id = req->candidate_id;
    
    if (req->term > node->current_term) {
        raft_become_follower(node, req->term);
    }
    
    resp->term = node->current_term;
    resp->vote_granted = false;
    
    if (req->term < node->current_term) {
        pthread_rwlock_unlock(&node->lock);
        return false;
    }
    
    if (node->voted_for == 0 || node->voted_for == req->candidate_id) {
        uint64_t last_idx = raft_last_log_index(node);
        uint64_t last_term = raft_last_log_term(node);
        
        bool up_to_date = false;
        if (req->last_log_term > last_term) {
            up_to_date = true;
        } else if (req->last_log_term == last_term && req->last_log_index >= last_idx) {
            up_to_date = true;
        }
        
        if (up_to_date) {
            node->voted_for = req->candidate_id;
            resp->vote_granted = true;
        }
    }
    
    pthread_rwlock_unlock(&node->lock);
    return resp->vote_granted;
}

/* AppendEntries handler */
static bool raft_handle_append_entries(raft_node_t *node, const raft_msg_append_t *req, raft_msg_append_t *resp) {
    pthread_rwlock_wrlock(&node->lock);
    
    resp->leader_id = req->leader_id;
    
    if (req->term > node->current_term) {
        raft_become_follower(node, req->term);
    }
    
    resp->term = node->current_term;
    resp->success = false;
    
    if (req->term < node->current_term) {
        pthread_rwlock_unlock(&node->lock);
        return false;
    }
    
    /* Reset election timeout logic here in full engine */
    if (node->role == RAFT_CANDIDATE) {
        node->role = RAFT_FOLLOWER;
    }
    
    /* Check log consistency at prev_log_index */
    if (req->prev_log_index > 0) {
        raft_entry_t *prev_entry = raft_get_log(node, req->prev_log_index);
        if (!prev_entry || prev_entry->term != req->prev_log_term) {
            resp->match_index = raft_last_log_index(node);
            pthread_rwlock_unlock(&node->lock);
            return false;
        }
    }
    
    /* Process entries and resolve conflicts */
    for (size_t i = 0; i < req->entries_count; i++) {
        uint64_t target_index = req->prev_log_index + 1 + i;
        raft_entry_t *existing = raft_get_log(node, target_index);
        
        if (existing) {
            if (existing->term != req->entries[i].term) {
                /* Conflict: truncate log from target_index */
                size_t keep_count = target_index - node->log_startIndex;
                node->log_count = keep_count;
                /* Append new entry */
                if (node->log_count < RAFT_MAX_LOGS) {
                    node->log[node->log_count++] = req->entries[i];
                }
            }
            /* If existing matches term, do nothing (idempotent) */
        } else {
            /* Append */
            if (target_index == node->log_startIndex + node->log_count) {
                if (node->log_count < RAFT_MAX_LOGS) {
                    node->log[node->log_count++] = req->entries[i];
                }
            }
        }
    }
    
    /* Update commit index */
    if (req->leader_commit > node->commit_index) {
        uint64_t last_new_index = req->prev_log_index + req->entries_count;
        node->commit_index = (req->leader_commit < last_new_index) ? req->leader_commit : last_new_index;
    }
    
    resp->success = true;
    resp->match_index = raft_last_log_index(node);
    
    pthread_rwlock_unlock(&node->lock);
    return true;
}

static uint64_t raft_append_local_entry(raft_node_t *node, const char *data, size_t len) {
    pthread_rwlock_wrlock(&node->lock);
    if (node->role != RAFT_LEADER) {
        pthread_rwlock_unlock(&node->lock);
        return 0;
    }
    if (node->log_count >= RAFT_MAX_LOGS) {
        pthread_rwlock_unlock(&node->lock);
        return 0;
    }
    uint64_t new_idx = raft_last_log_index(node) + 1;
    raft_entry_t *entry = &node->log[node->log_count++];
    entry->term = node->current_term;
    entry->index = new_idx;
    entry->type = 0;
    entry->data_len = len < 256 ? len : 255;
    memcpy(entry->data, data, entry->data_len);
    entry->data[entry->data_len] = '\0';
    
    pthread_rwlock_unlock(&node->lock);
    return new_idx;
}

/* =========================================================================
 * STRESS TEST SUITE INFRASTRUCTURE
 * ========================================================================= */

#define TEST_ASSERT(cond, msg) do { \
    if (!(cond)) { \
        fprintf(stderr, "\033[31m[ASSERT FAIL]\033[0m %s:%d - Assertion failed: %s\n", __FILE__, __LINE__, msg); \
        exit(EXIT_FAILURE); \
    } \
} while(0)

static volatile int g_test_running = 1;

/* -------------------------------------------------------------------------
 * 1. UNIT & BOUNDARY TESTS
 * ------------------------------------------------------------------------- */
static void test_unit_boundaries(void) {
    printf("[RUN ] test_unit_boundaries...\n");
    raft_node_t node;
    raft_node_init(&node, 1);

    /* Invariant: New node must be follower, term 0, no votes */
    TEST_ASSERT(node.role == RAFT_FOLLOWER, "New node must be follower");
    TEST_ASSERT(node.current_term == 0, "Initial term must be 0");
    TEST_ASSERT(node.voted_for == 0, "Initial voted_for must be 0");
    TEST_ASSERT(raft_last_log_index(&node) == 0, "Initial last log index must be 0");

    /* Boundary: RequestVote with higher term forces step down and updates term */
    raft_msg_vote_t vote_req = { .term = 5, .candidate_id = 2, .last_log_index = 0, .last_log_term = 0 };
    raft_