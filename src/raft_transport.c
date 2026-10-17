#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <errno.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>

#define RAFT_TRANSPORT_MAGIC 0x5241465454524e53ULL // "RAFTTRNS"
#define MAX_MESSAGE_SIZE 1048576 // 1MB

typedef enum {
    RAFT_MSG_REQUEST_VOTE = 1,
    RAFT_MSG_REQUEST_VOTE_RESP = 2,
    RAFT_MSG_APPEND_ENTRIES = 3,
    RAFT_MSG_APPEND_ENTRIES_RESP = 4,
    RAFT_MSG_INSTALL_SNAPSHOT = 5,
    RAFT_MSG_INSTALL_SNAPSHOT_RESP = 6
} RaftMsgType;

typedef struct __attribute__((aligned(64))) {
    uint64_t magic;
    uint32_t msg_type;
    uint32_t payload_len;
    uint32_t crc32;
    uint32_t reserved;
} RaftHeader;

typedef struct {
    RaftHeader header;
    uint8_t payload[];
} RaftMessage;

typedef struct {
    int socket_fd;
    pthread_mutex_t write_mutex;
    pthread_mutex_t read_mutex;
    bool active;
    uint32_t peer_id;
} RaftTransportPeer;

typedef struct {
    RaftTransportPeer* peers;
    size_t peer_count;
    int listen_fd;
    pthread_t listener_thread;
    bool running;
    pthread_mutex_t transport_mutex;
} RaftTransportContext;

static uint32_t calculate_crc32(const uint8_t *data, size_t length) {
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < length; i++) {
        crc ^= data[i];
        for (uint32_t j = 0; j < 8; j++) {
            if (crc & 1) {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc = crc >> 1;
            }
        }
    }
    return ~crc;
}

static int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

bool raft_transport_init(RaftTransportContext *ctx, int port, size_t max_peers) {
    if (!ctx) return false;

    ctx->peer_count = max_peers;
    ctx->peers = (RaftTransportPeer*)calloc(max_peers, sizeof(RaftTransportPeer));
    if (!ctx->peers) return false;

    if (pthread_mutex_init(&ctx->transport_mutex, NULL) != 0) {
        free(ctx->peers);
        return false;
    }

    ctx->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (ctx->listen_fd == -1) {
        pthread_mutex_destroy(&ctx->transport_mutex);
        free(ctx->peers);
        return false;
    }

    int opt = 1;
    setsockopt(ctx->listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in address;
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (bind(ctx->listen_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        close(ctx->listen_fd);
        pthread_mutex_destroy(&ctx->transport_mutex);
        free(ctx->peers);
        return false;
    }

    if (listen(ctx->listen_fd, 128) < 0) {
        close(ctx->listen_fd);
        pthread_mutex_destroy(&ctx->transport_mutex);
        free(ctx->peers);
        return false;
    }

    set_nonblocking(ctx->listen_fd);
    ctx->running = true;
    return true;
}

void raft_transport_shutdown(RaftTransportContext *ctx) {
    if (!ctx) return;

    pthread_mutex_lock(&ctx->transport_mutex);
    ctx->running = false;
    if (ctx->listen_fd != -1) {
        close(ctx->listen_fd);
        ctx->listen_fd = -1;
    }

    for (size_t i = 0; i < ctx->peer_count; i++) {
        if (ctx->peers[i].active) {
            pthread_mutex_lock(&ctx->peers[i].write_mutex);
            close(ctx->peers[i].socket_fd);
            ctx->peers[i].socket_fd = -1;
            ctx->peers[i].active = false;
            pthread_mutex_unlock(&ctx->peers[i].write_mutex);
            pthread_mutex_destroy(&ctx->peers[i].write_mutex);
            pthread_mutex_destroy(&ctx->peers[i].read_mutex);
        }
    }

    free(ctx->peers);
    ctx->peers = NULL;
    pthread_mutex_unlock(&ctx->transport_mutex);
    pthread_mutex_destroy(&ctx->transport_mutex);
}

bool raft_transport_connect_peer(RaftTransportContext *ctx, uint32_t peer_id, const char *ip, int port) {
    if (!ctx || !ip) return false;

    pthread_mutex_lock(&ctx->transport_mutex);

    if (peer_id >= ctx->peer_count) {
        pthread_mutex_unlock(&ctx->transport_mutex);
        return false;
    }

    RaftTransportPeer *peer = &ctx->peers[peer_id];
    
    if (peer->active) {
        pthread_mutex_unlock(&ctx->transport_mutex);
        return true;
    }

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        pthread_mutex_unlock(&ctx->transport_mutex);
        return false;
    }

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, ip, &serv_addr.sin_addr) <= 0) {
        close(sock);
        pthread_mutex_unlock(&ctx->transport_mutex);
        return false;
    }

    if (connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        close(sock);
        pthread_mutex_unlock(&ctx->transport_mutex);
        return false;
    }

    set_nonblocking(sock);

    peer->socket_fd = sock;
    peer->peer_id = peer_id;
    peer->active = true;
    pthread_mutex_init(&peer->write_mutex, NULL);
    pthread_mutex_init(&peer->read_mutex, NULL);

    pthread_mutex_unlock(&ctx->transport_mutex);
    return true;
}

bool raft_transport_send(RaftTransportContext *ctx, uint32_t peer_id, RaftMsgType type, const uint8_t *payload, uint32_t payload_len) {
    if (!ctx || (payload_len > 0 && !payload)) return false;

    pthread_mutex_lock(&ctx->transport_mutex);
    if (peer_id >= ctx->peer_count || !ctx->peers[peer_id].active) {
        pthread_mutex_unlock(&ctx->transport_mutex);
        return false;
    }

    RaftTransportPeer *peer = &ctx->peers[peer_id];
    pthread_mutex_lock(&peer->write_mutex);
    pthread_mutex_unlock(&ctx->transport_mutex);

    if (payload_len > MAX_MESSAGE_SIZE) {
        pthread_mutex_unlock(&peer->write_mutex);
        return false;
    }

    size_t total_size = sizeof(RaftHeader) + payload_len;
    uint8_t *buffer = (uint8_t*)malloc(total_size);
    if (!buffer) {
        pthread_mutex_unlock(&peer->write_mutex);
        return false;
    }

    RaftHeader *header = (RaftHeader*)buffer;
    header->magic = RAFT_TRANSPORT_MAGIC;
    header->msg_type = (uint32_t)type;
    header->payload_len = payload_len;
    header->crc32 = calculate_crc32(payload, payload_len);
    header->reserved = 0;

    if (payload_len > 0) {
        memcpy(buffer + sizeof(RaftHeader), payload, payload_len);
    }

    size_t bytes_sent = 0;
    while (bytes_sent < total_size) {
        ssize_t written = write(peer->socket_fd, buffer + bytes_sent, total_size - bytes_sent);
        if (written < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                usleep(1000);
                continue;
            }
            free(buffer);
            pthread_mutex_unlock(&peer->write_mutex);
            return false;
        }
        bytes_sent += written;
    }

    free(buffer);
    pthread_mutex_unlock(&peer->write_mutex);
    return true;
}

bool raft_transport_receive(RaftTransportContext *ctx, uint32_t peer_id, RaftHeader *out_header, uint8_t **out_payload) {
    if (!ctx || !out_header || !out_payload) return false;

    pthread_mutex_lock(&ctx->transport_mutex);
    if (peer_id >= ctx->peer_count || !ctx->peers[peer_id].active) {
        pthread_mutex_unlock(&ctx->transport_mutex);
        return false;
    }

    RaftTransportPeer *peer = &ctx->peers[peer_id];
    pthread_mutex_lock(&peer->read_mutex);
    pthread_mutex_unlock(&ctx->transport_mutex);

    size_t header_bytes_read = 0;
    uint8_t *header_ptr = (uint8_t*)out_header;

    while (header_bytes_read < sizeof(RaftHeader)) {
        ssize_t n = read(peer->socket_fd, header_ptr + header_bytes_read, sizeof(RaftHeader) - header_bytes_read);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                pthread_mutex_unlock(&peer->read_mutex);
                return false;
            }
            pthread_mutex_unlock(&peer->read_mutex);
            return false;
        } else if (n == 0) {
            pthread_mutex_unlock(&peer->read_mutex);
            return false;
        }
        header_bytes_read += n;
    }

    if (out_header->magic != RAFT_TRANSPORT_MAGIC) {
        pthread_mutex_unlock(&peer->read_mutex);
        return false;
    }

    if (out_header->payload_len > MAX_MESSAGE_SIZE) {
        pthread_mutex_unlock(&peer->read_mutex);
        return false;
    }

    if (out_header->payload_len > 0) {
        *out_payload = (uint8_t*)malloc(out_header->payload_len);
        if (!*out_payload) {
            pthread_mutex_unlock(&peer->read_mutex);
            return false;
        }

        size_t payload_bytes_read = 0;
        while (payload_bytes_read < out_header->payload_len) {
            ssize_t n = read(peer->socket_fd, *out_payload + payload_bytes_read, out_header->payload_len - payload_bytes_read);
            if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    usleep(1000);
                    continue;
                }
                free(*out_payload);
                *out_payload = NULL;
                pthread_mutex_unlock(&peer->read_mutex);
                return false;
            } else if (n == 0) {
                free(*out_payload);
                *out_payload = NULL;
                pthread_mutex_unlock(&peer->read_mutex);
                return false;
            }
            payload_bytes_read += n;
        }

        uint32_t computed_crc = calculate_crc32(*out_payload, out_header->payload_len);
        if (computed_crc != out_header->crc32) {
            free(*out_payload);
            *out_payload = NULL;
            pthread_mutex_unlock(&peer->read_mutex);
            return false;
        }
    } else {
        *out_payload = NULL;
    }

    pthread_mutex_unlock(&peer->read_mutex);
    return true;
}