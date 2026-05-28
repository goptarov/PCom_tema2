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

using namespace std;

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

int send_data(int conn_id, char *buffer, int len)
{
    int size = 0;

    pthread_mutex_lock(&cons[conn_id]->con_lock);

    /* We will write code here as to not have sync problems with sender_handler */
    struct connection *con = cons[conn_id];

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
        int conn_id = -1;
        do {
            res = recv_message_or_timeout(buf, MAX_SEGMENT_SIZE, &conn_id);
        } while(res == -14);

        pthread_mutex_lock(&cons[conn_id]->con_lock);

        /* Handle segment received from the receiver. We use this between locks
        as to not have synchronization issues with the send_data calls which are
        on the main thread */

        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }
}

int setup_connection(uint32_t ip, uint16_t port) {
    /* Implement the sender part of the Three Way Handshake. Blocks
    until the connection is established */

    struct connection *con = (struct connection *)malloc(sizeof(struct connection));
    int conn_id = 0;
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (con->sockfd == -1) {
        DEBUG_PRINT("socket creation failed\n");
    }

    struct sockaddr_in server_addr;
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

    struct poli_synack synack;
    while (1) {
        DEBUG_PRINT("Sending SYN to %s: %d\n", inet_ntoa(server_addr.sin_addr), ntohs(server_addr.sin_port));
        sendto(con->sockfd, &syn, sizeof(syn), 0, (struct sockaddr *)&server_addr, sizeof(server_addr));

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

    struct poli_tcp_ctrl_hdr ack;
    ack.protocol_id = POLI_PROTOCOL_ID;
    ack.conn_id = conn_id;
    ack.type = ACK;
    ack.ack_num = 0;
    ack.recv_window = 0;

    DEBUG_PRINT("Sending ACK to %s: %d\n", inet_ntoa(con->servaddr.sin_addr), ntohs(con->servaddr.sin_port));
    sendto(con->sockfd, &ack, sizeof(ack), 0, (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));

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
    pthread_t thread1;
    int ret;

    /* Create a thread that will*/
    ret = pthread_create( &thread1, NULL, sender_handler, NULL);
    assert(ret == 0);
}
