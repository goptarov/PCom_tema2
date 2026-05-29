
#pragma once

#include <cstdint>
#include "utils.h"
#include <arpa/inet.h>

#include "protocol.h"

/* Maximum segment size, change as you see fit */
#define MAX_DATA_SIZE 512
#define MAX_SEGMENT_SIZE (MAX_DATA_SIZE + sizeof(poli_tcp_data_hdr))

//Size for the buffers to which we write and from which we read
#define MAX_BUF_SIZE (1024 * 1024)
#define MAX_WINDOW_SIZE 64
#define MAX_CONNECTIONS 32

/* Protocol control block. Used track different parameters about a connection. 
 * Will need to be extenden to solve the homework with other parameters such as
 * last_ack or status depending on how you implement your protocol. */
struct connection {
    /* common window for both the sender and receiver. */
    /* list window: A window representation */
    int sockfd; /* socket used for this connection */
    int conn_id; /* connection identifier */
    struct sockaddr_in servaddr; /* used to identify the destination */
    pthread_mutex_t con_lock; /* Used for syncronization with the handler thread and read/send calls.*/

    int max_window_seq; /* Used to store the max number of packets that can be inflight, since we can
                           have many more packets in our window */

    /* TODO. Parameters used only by the sender */
    char send_buffer[MAX_BUF_SIZE];
    int send_buffer_len;
    int next_seq;
    int last_acked_seq;

    //We need segment copies for retransmission.
    char segment_copies[MAX_WINDOW_SIZE][MAX_SEGMENT_SIZE];
    int segment_copies_lengths[MAX_WINDOW_SIZE];

    /* TODO. Parameters used only by the client */
    char recv_buffer[MAX_BUF_SIZE];
    int recv_buffer_len;
};

/* ########## API that we expose to the application ########### */

/* Equivalent of listen. Ran by the server to waits for a connection from a
 * client. Returns a connection id. Blocking untill it receives a connection
 * request */
int wait4connect(uint32_t ip, uint16_t port);
/* Equivalent of connect. Used by the client to connect to a server. */
int setup_connection(uint32_t ip, uint16_t port);
/* Equivalent to recv. Blocking if there is no data to be written in buffer */
int recv_data(int connectionid, char *buffer, int len);
/* Equivalent to send. Used by the client to send a stream of bytes as segments */
int send_data(int conn_id, char *buffer, int len);
/* Used to initialize your protocol on the receiver side. */
void init_receiver(int recv_buffer_bytes);
/* Used to initialize your protocol on the sender side */
void init_sender(int speed, int delay);

/* ######### Internal API used by sender and receiver ########### */
int recv_message_or_timeout(char *buff, size_t len, int *conn_id);
