#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include <stdbool.h>
#include <errno.h>

#define RAFT_ARENA_ALIGNMENT 64
#define RAFT_ARENA_DEFAULT_BLOCK_SIZE (1024 * 1024)

typedef struct ArenaBlock {
    struct ArenaBlock* next;
    size_t capacity;
    size_t used;
    _Alignas(64) uint8_t data[];
} ArenaBlock;

typedef struct __attribute__((aligned(64))) RaftArena {
    ArenaBlock* current;
    ArenaBlock* head;
    size_t block_size;
    pthread_mutex_t lock;
    size_t total_allocated_bytes;
    size_t block_count;
} RaftArena;

static inline size_t raft_align_forward(size_t ptr, size_t alignment) {
    size_t modulo = ptr & (alignment - 1);
    if (modulo == 0) {
        return ptr;
    }
    return ptr + (alignment - modulo);
}

RaftArena* raft_arena_create(size_t block_size) {
    size_t actual_block_size = block_size > 0 ? block_size : RAFT_ARENA_DEFAULT_BLOCK_SIZE;
    size_t total_size = sizeof(RaftArena) + actual_block_size;
    
    ArenaBlock* initial_block = (ArenaBlock*)malloc(total_size);
    if (!initial_block) {
        return NULL;
    }

    RaftArena* arena = (RaftArena*)initial_block;
    ArenaBlock* block = (ArenaBlock*)((uint8_t*)initial_block + sizeof(RaftArena));
    
    block->next = NULL;
    block->capacity = actual_block_size;
    block->used = 0;

    arena->current = block;
    arena->head = block;
    arena->block_size = actual_block_size;
    arena->total_allocated_bytes = actual_block_size;
    arena->block_count = 1;

    if (pthread_mutex_init(&arena->lock, NULL) != 0) {
        free(initial_block);
        return NULL;
    }

    return arena;
}

void* raft_arena_alloc(RaftArena* arena, size_t size) {
    if (!arena || size == 0) {
        return NULL;
    }

    size_t aligned_size = raft_align_forward(size, RAFT_ARENA_ALIGNMENT);

    pthread_mutex_lock(&arena->lock);

    ArenaBlock* block = arena->current;
    while (block) {
        size_t current_used = block->used;
        size_t aligned_offset = raft_align_forward((size_t)block->data + current_used, RAFT_ARENA_ALIGNMENT) - (size_t)block->data;
        
        if (aligned_offset + aligned_size <= block->capacity) {
            void* ptr = (void*)(block->data + aligned_offset);
            block->used = aligned_offset + aligned_size;
            pthread_mutex_unlock(&arena->lock);
            memset(ptr, 0, size);
            return ptr;
        }
        block = block->next;
    }

    size_t new_block_capacity = arena->block_size;
    if (aligned_size > new_block_capacity) {
        new_block_capacity = raft_align_forward(aligned_size, RAFT_ARENA_ALIGNMENT);
    }

    size_t alloc_size = sizeof(ArenaBlock) + new_block_capacity;
    ArenaBlock* new_block = (ArenaBlock*)malloc(alloc_size);
    if (!new_block) {
        pthread_mutex_unlock(&arena->lock);
        return NULL;
    }

    new_block->next = NULL;
    new_block->capacity = new_block_capacity;
    
    size_t aligned_offset = raft_align_forward((size_t)new_block->data, RAFT_ARENA_ALIGNMENT) - (size_t)new_block->data;
    void* ptr = (void*)(new_block->data + aligned_offset);
    new_block->used = aligned_offset + aligned_size;

    new_block->next = arena->head;
    arena->head = new_block;
    arena->current = new_block;

    arena->total_allocated_bytes += new_block_capacity;
    arena->block_count++;

    pthread_mutex_unlock(&arena->lock);
    memset(ptr, 0, size);
    return ptr;
}

void raft_arena_reset(RaftArena* arena) {
    if (!arena) {
        return;
    }

    pthread_mutex_lock(&arena->lock);

    ArenaBlock* block = arena->head;
    while (block) {
        block->used = 0;
        block = block->next;
    }
    arena->current = arena->head;

    pthread_mutex_unlock(&arena->lock);
}

void raft_arena_destroy(RaftArena* arena) {
    if (!arena) {
        return;
    }

    pthread_mutex_lock(&arena->lock);
    ArenaBlock* block = arena->head;
    while (block) {
        ArenaBlock* next = block->next;
        if (block != (ArenaBlock*)arena) {
            free(block);
        }
        block = next;
    }
    pthread_mutex_unlock(&arena->lock);

    pthread_mutex_destroy(&arena->lock);
    free(arena);
}

size_t raft_arena_bytes_allocated(const RaftArena* arena) {
    if (!arena) {
        return 0;
    }
    return arena->total_allocated_bytes;
}