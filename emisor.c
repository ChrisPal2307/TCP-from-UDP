#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <errno.h>

#include "protocol.h"

#define TIMEOUT_SEC 1     // Wait time for timeout (1 second)
#define MAX_RETRIES 15    // Maximum retries before aborting

// Send a packet and wait for its ACK
int send_packet_and_wait_ack(int sockfd, struct sockaddr_in *server_addr, socklen_t addr_len, Packet *pkt) {
    AckPacket ack;
    int retries = 0;

    while (retries < MAX_RETRIES) {
        // Send the packet	
        if (sendto(sockfd, pkt, sizeof(Packet), 0, (struct sockaddr *)server_addr, addr_len) < 0) {
            perror("Error sending packet");
            return -1;
        }

        // Wait for ACK
        ssize_t bytes_rcvd = recvfrom(sockfd, &ack, sizeof(AckPacket), 0, NULL, NULL);

        if (bytes_rcvd > 0) {
            // Verify that it is a valid ACK matching our packet sequence and file_id
            if (ack.type == TYPE_ACK && ack.ack_num == pkt->seq_num && ack.file_id == pkt->file_id) {
                return 0; 
            }
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            printf("[TIMEOUT] Retransmitting packet seq=%u (attempt %d/%d)...\n", pkt->seq_num, retries + 1, MAX_RETRIES);
            retries++;
        } else {
            perror("Error in recvfrom");
            return -1;
        }
    }
    fprintf(stderr, "Error: Exceeded maximum retries for seq=%u\n", pkt->seq_num);
    return -1;
}

int main(int argc, char *argv[]) {
    if (argc < 5) {
        fprintf(stderr, "Usage: %s <Receiver IP> <Receiver Port> <File Path> <File ID>\n", argv[0]);
        fprintf(stderr, "Example: %s 127.0.0.1 8080 large_file.txt 1\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    char *dest_ip = argv[1];
    int dest_port = atoi(argv[2]);
    char *filepath = argv[3];
    uint32_t file_id = (uint32_t)atoi(argv[4]);

    // Open local file
    FILE *file = fopen(filepath, "rb");
    if (!file) {
        perror("Error opening local file");
        exit(EXIT_FAILURE);
    }

    // Create UDP socket
    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) {
        perror("Error creating UDP socket");
        fclose(file);
        exit(EXIT_FAILURE);
    }

    // Configure receiver address structure
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(dest_port);
    inet_pton(AF_INET, dest_ip, &server_addr.sin_addr);

    // Configure Socket Timeout
    struct timeval tv;
    tv.tv_sec = TIMEOUT_SEC;
    tv.tv_usec = 0;
    if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        perror("Error setting socket timeout");
        fclose(file);
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    socklen_t addr_len = sizeof(server_addr);
    Packet pkt;
    uint32_t seq_num = 0;

    printf("=== Starting Transmission (File ID: %u) ===\n", file_id);

    // Send SYN
    memset(&pkt, 0, sizeof(Packet));
    pkt.type = TYPE_SYN;
    pkt.seq_num = seq_num;
    pkt.file_id = file_id;
    
    // Pass filename in data buffer so receiver knows what file to create
    strncpy(pkt.data, filepath, CHUNK_SIZE - 1);
    pkt.length = (uint16_t)strlen(pkt.data);

    printf("[SYN] Establishing connection...\n");
    if (send_packet_and_wait_ack(sockfd, &server_addr, addr_len, &pkt) < 0) {
        fprintf(stderr, "Failed to send SYN packet\n");
        fclose(file);
        close(sockfd);
        exit(EXIT_FAILURE);
    }
    seq_num++;


    // Send File Chunks 
    size_t bytes_read = 0;
    while ((bytes_read = fread(pkt.data, 1, CHUNK_SIZE, file)) > 0) {
        pkt.type = TYPE_DATA;
        pkt.seq_num = seq_num;
        pkt.file_id = file_id;
        pkt.length = (uint16_t)bytes_read;

        if (send_packet_and_wait_ack(sockfd, &server_addr, addr_len, &pkt) < 0) {
            fprintf(stderr, "Transmission failed at seq=%u\n", seq_num);
            fclose(file);
            close(sockfd);
            exit(EXIT_FAILURE);
        }
        seq_num++;
    }
    
    // Send FIN 
    memset(&pkt, 0, sizeof(Packet));
    pkt.type = TYPE_FIN;
    pkt.seq_num = seq_num;
    pkt.file_id = file_id;
    pkt.length = 0;

    printf("[FIN] Closing connection...\n");
    if (send_packet_and_wait_ack(sockfd, &server_addr, addr_len, &pkt) < 0) {
        fprintf(stderr, "Failed to send FIN packet\n");
    } else {
        printf("=== Transmission Completed Successfully ===\n");
    }

    // Cleanup
    fclose(file);
    close(sockfd);
    return 0;
}
