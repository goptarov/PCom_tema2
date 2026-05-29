#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include <cassert>
#include <cstring>
#include <poll.h>
#include <sys/timerfd.h>

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;
int window_size = 1;

int send_data(int conn_id, char *buffer, int len)
{
    int size = 0;

    pthread_mutex_lock(&cons[conn_id]->con_lock);

    /* We will write code here as to not have sync problems with sender_handler */
    connection *con = cons[conn_id];

    if (len > MAX_BUF_SIZE - con->send_buffer_len) {
        memcpy(con->send_buffer + con->send_buffer_len, buffer, MAX_BUF_SIZE - con->send_buffer_len);
        size = MAX_BUF_SIZE - con->send_buffer_len;
        con->send_buffer_len = MAX_BUF_SIZE;
    }
    else {
        memcpy(con->send_buffer + con->send_buffer_len, buffer, len);
        size = len;
        con->send_buffer_len += len;
    }

    pthread_mutex_unlock(&cons[conn_id]->con_lock);

    return size;
}

void *sender_handler(void *arg)
{
    int res = 0;
    char buf[MAX_SEGMENT_SIZE];

    while (1) {

        if (cons.size() == 0) {
            continue;
        }

        for (std::pair<const int, connection *> &pair : cons) {
            connection *con = pair.second;

            pthread_mutex_lock(&con->con_lock);

            //Send window_size segments, increasing next_seq after every sent segment. last_acked_seq remains constant
            //here until all are sent
            while (con->next_seq < con->last_acked_seq + window_size && con->send_buffer_len > 0) {

                int segment_len = con->send_buffer_len > MAX_DATA_SIZE ? MAX_DATA_SIZE : con->send_buffer_len;

                poli_tcp_data_hdr segment_hdr;
                segment_hdr.protocol_id = POLI_PROTOCOL_ID;
                segment_hdr.conn_id = con->conn_id;
                segment_hdr.type = DATA;
                segment_hdr.seq_num = con->next_seq;
                segment_hdr.len = segment_len;

                memcpy(&con->segment_copies[segment_hdr.seq_num], &segment_hdr, sizeof(segment_hdr));
                memcpy(&con->segment_copies[segment_hdr.seq_num] + sizeof(segment_hdr), &con->send_buffer, segment_len);
                con->send_buffer_len -= segment_len;

                //Move what is left unread from send_buffer to the beggining
                memcpy(&con->send_buffer, &con->send_buffer + segment_len, segment_len);

                con->segment_copies_lengths[segment_hdr.seq_num] = sizeof(segment_hdr) + segment_len;

                DEBUG_PRINT("Sending segment to %s: %d\n", inet_ntoa(con->servaddr.sin_addr), ntohs(con->servaddr.sin_port));
                sendto(con->sockfd, con->segment_copies[con->conn_id], con->segment_copies_lengths[con->conn_id], 0, (const sockaddr*)&con->servaddr, sizeof(con->servaddr));
                con->next_seq++;
            }

            pthread_mutex_unlock(&con->con_lock);
        }

        int conn_id = -1;
        do {
            //this looks for received segments from server.
            res = recv_message_or_timeout(buf, MAX_SEGMENT_SIZE, &conn_id);
        } while(res == -14);

        pthread_mutex_lock(&cons[conn_id]->con_lock);

        /* Handle segment received from the receiver. We use this between locks
        as to not have synchronization issues with the send_data calls which are
        on the main thread */
        connection *con = cons[conn_id];
        if (res != -1) {
            //Handle received ack
            poli_tcp_ctrl_hdr *ack = (poli_tcp_ctrl_hdr *)buf;
            if (ack->protocol_id == POLI_PROTOCOL_ID && ack->type == ACK) {
                DEBUG_PRINT("Received ACK\n");
                con->last_acked_seq = ntohs(ack->ack_num);
            }
        }
        else {
            //Handle timeout
            for (int i = con->last_acked_seq; i < con->next_seq; i++) {
                sendto(con->sockfd, con->segment_copies[i], con->segment_copies_lengths[i], 0, (const sockaddr*)&con->servaddr, sizeof(con->servaddr));
            }
        }

        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }
}

int setup_connection(uint32_t ip, uint16_t port) {
    /* Implement the sender part of the Three Way Handshake. Blocks
    until the connection is established */

    connection *con = (connection *)malloc(sizeof(connection));
    int conn_id = 0;
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (con->sockfd == -1) {
        DEBUG_PRINT("socket creation failed\n");
    }

    sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = ip;
    server_addr.sin_port = port;

    /* // This can be used to set a timer on a socket 
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 100000;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
        perror("Error");
    } */

    struct poli_tcp_ctrl_hdr syn;
    syn.protocol_id = POLI_PROTOCOL_ID;
    syn.conn_id = conn_id;
    syn.type = SYN;
    syn.ack_num = 0;
    syn.recv_window = 0;

    poli_synack synack;
    while (1) {
        DEBUG_PRINT("Sending SYN to %s: %d\n", inet_ntoa(server_addr.sin_addr), ntohs(server_addr.sin_port));
        sendto(con->sockfd, &syn, sizeof(syn), 0, (sockaddr *)&server_addr, sizeof(server_addr));

        recvfrom(con->sockfd, &synack, sizeof(synack), 0, NULL, NULL);

        if (synack.hdr.protocol_id == POLI_PROTOCOL_ID && synack.hdr.type == SYNACK) {
            DEBUG_PRINT("Received SYN+ACK\n");
            break;
        }
        else {
            DEBUG_PRINT("Expected SYN+ACK but got something else\n");
        }
    }
    server_addr.sin_port = synack.assigned_port; //already in network order
    con->servaddr = server_addr;
    con->conn_id = conn_id;

    poli_tcp_ctrl_hdr ack;
    ack.protocol_id = POLI_PROTOCOL_ID;
    ack.conn_id = conn_id;
    ack.type = ACK;
    ack.ack_num = 0;
    ack.recv_window = 0;

    DEBUG_PRINT("Sending ACK to %s: %d\n", inet_ntoa(con->servaddr.sin_addr), ntohs(con->servaddr.sin_port));
    sendto(con->sockfd, &ack, sizeof(ack), 0, (sockaddr *)&con->servaddr, sizeof(con->servaddr));

    /* We will send the SYN on 8031. Then we will receive a SYN-ACK with the connection
     * port. We can use con->sockfd for both cases, but we will need to update server_addr
     * with the port received via SYN-ACK */

    /* Since we can have multiple connection, we want to know if data is available
       on the socket used by a given connection. We use POLL for this */
    data_fds[fdmax].fd = con->sockfd;    
    data_fds[fdmax].events = POLLIN;    
    
    /* This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on our connection */
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME,  0);    
    timer_fds[fdmax].events = POLLIN;    
    struct itimerspec spec;
    spec.it_value.tv_sec = 1;    
    spec.it_value.tv_nsec = 0;    
    spec.it_interval.tv_sec = 1;    
    spec.it_interval.tv_nsec = 0;    
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);    
    fdmax++;

    pthread_mutex_init(&con->con_lock, NULL);
    cons.insert({conn_id, con});

    DEBUG_PRINT("Connection established!\n");

    return conn_id;
}

void init_sender(int speed, int delay)
{
    //speed (Mb/s), delay(ms)
    const int bdp = ((speed * 1000000) * (delay / 1000)) / 8; //bytes
    window_size = bdp / (int)MAX_SEGMENT_SIZE;
    if (window_size < 1) window_size = 1;
    else if (window_size > (int)MAX_SEGMENT_SIZE) window_size = MAX_SEGMENT_SIZE;

    DEBUG_PRINT("window size is %d\n", window_size);

    /* Create a thread that will*/
    pthread_t thread1;
    int ret = pthread_create( &thread1, NULL, sender_handler, NULL);
    assert(ret == 0);
}
