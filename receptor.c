#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <pthread.h>

#include "protocol.h"

#define MAX_SESSIONS 10
#define QUEUE_SIZE 256

// Node for incoming packet task
typedef struct {
    struct sockaddr_in client_addr;
    socklen_t client_len;
    Packet pkt;
} PacketTask;

// Thread-safe Queue per Sender Session
typedef struct {
    PacketTask tasks[QUEUE_SIZE];
    int head;
    int tail;
    int count;
    pthread_mutex_t queue_mutex;
    pthread_cond_t queue_cond;
} PacketQueue;

// Structure for a Dedicated Sender Session
typedef struct {
    uint32_t file_id;
    uint32_t expected_seq_num;
    FILE *file;
    char filename[256];
    int active;
    int sockfd;
    pthread_t worker_thread;
    PacketQueue queue;
} SenderSession;

// Global session table & synchronization lock
SenderSession sessions[MAX_SESSIONS];
pthread_mutex_t session_mutex = PTHREAD_MUTEX_INITIALIZER;

// Queue helper functions
void queue_init(PacketQueue *q) {
    q->head = 0;
    q->tail = 0;
    q->count = 0;
    pthread_mutex_init(&q->queue_mutex, NULL);
    pthread_cond_init(&q->queue_cond, NULL);
}

void queue_push(PacketQueue *q, PacketTask *task) {
    pthread_mutex_lock(&q->queue_mutex);
    if (q->count < QUEUE_SIZE) {
        q->tasks[q->tail] = *task;
        q->tail = (q->tail + 1) % QUEUE_SIZE;
        q->count++;
        pthread_cond_signal(&q->queue_cond);
    } else {
        fprintf(stderr, "[QUEUE] Warning: Queue full! Packet dropped for task.\n");
    }
    pthread_mutex_unlock(&q->queue_mutex);
}

PacketTask queue_pop(PacketQueue *q) {
    pthread_mutex_lock(&q->queue_mutex);
    while (q->count == 0) {
        pthread_cond_wait(&q->queue_cond, &q->queue_mutex);
    }
    PacketTask task = q->tasks[q->head];
    q->head = (q->head + 1) % QUEUE_SIZE;
    q->count--;
    pthread_mutex_unlock(&q->queue_mutex);
    return task;
}

// Forward declaration
void* sender_worker_thread(void *arg);

// Create or find a sender session
SenderSession* get_or_create_session(uint32_t file_id, const char *incoming_filename, int sockfd) {
    pthread_mutex_lock(&session_mutex);
    for (int i = 0; i < MAX_SESSIONS; i++) {
        if (sessions[i].active && sessions[i].file_id == file_id) {
            pthread_mutex_unlock(&session_mutex);
            return &sessions[i];
        }
    }
    for (int i = 0; i < MAX_SESSIONS; i++) {
        if (!sessions[i].active) {
            sessions[i].file_id = file_id;
            sessions[i].expected_seq_num = 1;
            sessions[i].active = 1;
            sessions[i].sockfd = sockfd;
            snprintf(sessions[i].filename, sizeof(sessions[i].filename), 
                     "received_id%u_%s", file_id, incoming_filename);
            sessions[i].file = fopen(sessions[i].filename, "wb");
            if (!sessions[i].file) {
                perror("Error creating local output file");
                sessions[i].active = 0;
                pthread_mutex_unlock(&session_mutex);
                return NULL;
            }
            queue_init(&sessions[i].queue);
            if (pthread_create(&sessions[i].worker_thread, NULL, sender_worker_thread, &sessions[i]) != 0) {
                perror("Error creating worker thread for file");
                fclose(sessions[i].file);
                sessions[i].active = 0;
                pthread_mutex_unlock(&session_mutex);
                return NULL;
            }
            printf("[SERVER] Dedicated thread created for file_id=%u -> Writing to '%s'\n", file_id, 
                                                                    sessions[i].filename);
            pthread_mutex_unlock(&session_mutex);
            return &sessions[i];
        }
    }
    pthread_mutex_unlock(&session_mutex);
    fprintf(stderr, "[SERVER] Error: Maximum concurrent sessions reached.\n");
    return NULL;
}


void* sender_worker_thread(void *arg) {
    SenderSession *session = (SenderSession *)arg;

    while (session->active) {
        PacketTask task = queue_pop(&session->queue);
        Packet *pkt = &task.pkt;

        if (pkt->type == TYPE_DATA) {

            if (pkt->seq_num == session->expected_seq_num) {
                fwrite(pkt->data, 1, pkt->length, session->file);
                session->expected_seq_num++;
                AckPacket ack = {.type = TYPE_ACK, .ack_num = pkt->seq_num, .file_id = pkt->file_id};
                sendto(session->sockfd, &ack, sizeof(AckPacket), 0, 
                       (struct sockaddr *)&task.client_addr, task.client_len);
            }

        } else if (pkt->type == TYPE_FIN) {
            AckPacket ack = {.type = TYPE_ACK, .ack_num = pkt->seq_num, .file_id = pkt->file_id};
            sendto(session->sockfd, &ack, sizeof(AckPacket), 0, 
                   (struct sockaddr *)&task.client_addr, task.client_len);

            printf("[FILE THREAD %u] Finished transfer. Closing file '%s'\n", 
                   session->file_id, session->filename);

            if (session->file) {
                fclose(session->file);
                session->file = NULL;
            }

            pthread_mutex_lock(&session_mutex);
            session->active = 0;
            pthread_mutex_unlock(&session_mutex);
            break; 
        }
    }

    pthread_exit(NULL);
}


int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <Listen Port>\n", argv[0]);
        fprintf(stderr, "Example: %s 8080\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    int port = atoi(argv[1]);
    memset(sessions, 0, sizeof(sessions));

    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) {
        perror("Error creating socket");
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port);

    if (bind(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("Error binding socket");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    printf("=== Multi-Threaded Receiver Running (1 Thread Per File) on Port %d ===\n", port);

    while (1) {
        PacketTask task;
        task.client_len = sizeof(task.client_addr);

        ssize_t bytes_rcvd = recvfrom(sockfd, &task.pkt, sizeof(Packet), 0,
                                     (struct sockaddr *)&task.client_addr, &task.client_len);
        if (bytes_rcvd < 0) {
            perror("Error receiving packet");
            continue;
        }
        if (task.pkt.type == TYPE_SYN) {
            SenderSession *session = get_or_create_session(task.pkt.file_id, task.pkt.data, sockfd);
            if (session) {
                AckPacket ack = {.type = TYPE_ACK, .ack_num = task.pkt.seq_num, .file_id = task.pkt.file_id};
                sendto(sockfd, &ack, sizeof(AckPacket), 0, 
                       (struct sockaddr *)&task.client_addr, task.client_len);}
            continue;
        }
        pthread_mutex_lock(&session_mutex);
        SenderSession *target_session = NULL;
        for (int i = 0; i < MAX_SESSIONS; i++) {
            if (sessions[i].active && sessions[i].file_id == task.pkt.file_id) {
                target_session = &sessions[i];
                break;}
        }
        if (!target_session) {
            pthread_mutex_unlock(&session_mutex);
            continue;}

        if (task.pkt.seq_num < target_session->expected_seq_num) {
        printf("[MAIN THREAD] Duplicate seq=%u received (expected seq=%u). Re-sending ACK.\n", 
                   task.pkt.seq_num, target_session->expected_seq_num);
            AckPacket ack = {.type = TYPE_ACK, .ack_num = task.pkt.seq_num, .file_id = task.pkt.file_id};
            sendto(sockfd, &ack, sizeof(AckPacket), 0, 
                   (struct sockaddr *)&task.client_addr, task.client_len);
            pthread_mutex_unlock(&session_mutex);
            continue;
        }
        pthread_mutex_unlock(&session_mutex);
        queue_push(&target_session->queue, &task);
    }

    close(sockfd);
    return 0;
}
