#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <poll.h>
#include <cassert>
#include <cstring>
#include <unistd.h>
#include <sys/timerfd.h>

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;
int listen_sockfd;

int recv_data(int conn_id, char *buffer, int len)
{
    int size = 0;

    pthread_mutex_lock(&cons[conn_id]->con_lock);
    
    /* We will write code here as to not have sync problems with recv_handler */
    connection *con = cons[conn_id];

    //blocking loop (otherwise this will read nothing and return 0 which will uselessly keep looping in server.cpp)
    while (con->recv_buffer_len == 0 && !con->transfer_done) {
        pthread_mutex_unlock(&cons[conn_id]->con_lock);
        usleep(1000);
        pthread_mutex_lock(&cons[conn_id]->con_lock);
    }

    if ((uint32_t)len < con->recv_buffer_len)
        size = len;
    else
        size = con->recv_buffer_len;

    memcpy(buffer, con->recv_buffer, size);
    con->recv_buffer_len -= size;

    //we have to move what is left unread to the beggining of the recv_buffer
    memmove(con->recv_buffer, con->recv_buffer + size, con->recv_buffer_len);

    pthread_mutex_unlock(&cons[conn_id]->con_lock);

    return size;
}

void *receiver_handler(void *arg)
{
    char segment[MAX_SEGMENT_SIZE];
    int res;
    DEBUG_PRINT("Starting receiver handler\n");

    while (1) {

        int conn_id = -1;
        do {
            res = recv_message_or_timeout(segment, MAX_SEGMENT_SIZE, &conn_id);
        } while(res == -14);

        pthread_mutex_lock(&cons[conn_id]->con_lock);

        /* Handle segment received from the sender. We use this between locks
        as to not have synchronization issues with the recv_data calls which are
        on the main thread */
        connection *con = cons[conn_id];
        if (res != -1) {
            poli_tcp_data_hdr *segment_hdr = (poli_tcp_data_hdr *)segment;
            if (segment_hdr->protocol_id == POLI_PROTOCOL_ID && segment_hdr->type == DATA) {
                uint16_t seq = ntohs(segment_hdr->seq_num);
                uint16_t len = ntohs(segment_hdr->len);

                //only read the segment with the expected seq, otherwise drop.
                if (seq == con->next_expected_seq && con->recv_buffer_len + len <= MAX_BUF_SIZE) {
                    memcpy(con->recv_buffer + con->recv_buffer_len, segment + sizeof(poli_tcp_data_hdr), len);
                    con->recv_buffer_len += len;
                    con->next_expected_seq++;
                    DEBUG_PRINT("Delivered seq %u (%u bytes)\n", seq, len);
                }
                else {
                    DEBUG_PRINT("Dropped seq %u (expected %u, buf %u/%u)\n", seq, con->next_expected_seq, con->recv_buffer_len, MAX_BUF_SIZE);
                }

                uint32_t free_space = MAX_BUF_SIZE - con->recv_buffer_len;
                uint16_t truncated = (free_space > 0xFFFF) ? 0xFFFF : free_space;

                poli_tcp_ctrl_hdr ack;
                ack.protocol_id = POLI_PROTOCOL_ID;
                ack.conn_id = conn_id;
                ack.type = ACK;
                ack.ack_num = htons(con->next_expected_seq);
                ack.recv_window = htons(truncated);

                sendto(con->sockfd, &ack, sizeof(ack), 0, (const sockaddr *)&con->servaddr, sizeof(con->servaddr));
            }
            else if (segment_hdr->protocol_id == POLI_PROTOCOL_ID && segment_hdr->type == FIN) {
                DEBUG_PRINT("Received FIN on conn %d, sending FINACK\n", conn_id);

                poli_tcp_ctrl_hdr finack;
                finack.protocol_id = POLI_PROTOCOL_ID;
                finack.conn_id = conn_id;
                finack.type = FINACK;
                finack.ack_num = 0;
                finack.recv_window = 0;

                // Send multiple times to make sure it gets through
                for (int i = 0; i < 5; i++)
                    sendto(con->sockfd, &finack, sizeof(finack), 0, (const sockaddr*)&con->servaddr, sizeof(con->servaddr));

                con->transfer_done = 1;
            }
        }
        else {
            //Timeout
            uint32_t free_space = MAX_BUF_SIZE - con->recv_buffer_len;
            uint16_t truncated = (free_space > 0xFFFF) ? 0xFFFF : free_space;

            poli_tcp_ctrl_hdr ack;
            ack.protocol_id = POLI_PROTOCOL_ID;
            ack.conn_id = conn_id;
            ack.type = ACK;
            ack.ack_num = htons(con->next_expected_seq);
            ack.recv_window = htons(truncated);

            sendto(con->sockfd, &ack, sizeof(ack), 0, (const sockaddr *)&con->servaddr, sizeof(con->servaddr));
            DEBUG_PRINT("Receiver timeout conn %d: re-ACK %u\n", conn_id, con->next_expected_seq);
        }
        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }
}

int wait4connect(uint32_t ip, uint16_t port)
{
    /* TODO: Implement the Three Way Handshake on the receiver part. This blocks
     * until a connection is established. */
    int ret = 0;
    sockaddr_in client_addr;
    socklen_t client_addr_len = sizeof(client_addr);
    char buf[MAX_SEGMENT_SIZE];

    connection *con = (connection *)malloc(sizeof(connection));
    memset(con, 0, sizeof(connection));
    static int next_conn_id = 0;
    int conn_id = next_conn_id++;

    /* This can be used to set a timer on a socket, useful once we received a
     * SYN. You may want to disable by setting the time to 0 (tv_sec = 0,
     * tv_usec = 0)
     */

    /* Receive SYN on the connection socket. Create a new socket and bind it to
     * the chosen port. Send the data port number via SYN-ACK to the client */
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (con->sockfd == -1) {
        DEBUG_PRINT("Couldn't create client socket\n");
        exit(-1);
    }
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 10000;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        perror("Error");
    }

    recvfrom(listen_sockfd, buf, sizeof(buf), 0, (sockaddr *)&client_addr, &client_addr_len);
    DEBUG_PRINT("Recieved SYN from %s: %d\n", inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));

    sockaddr_in server_new_addr;
    memset(&server_new_addr, 0, sizeof(sockaddr_in));
    server_new_addr.sin_family = AF_INET;
    server_new_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    do {
        server_new_addr.sin_port = htons(1024 + rand() % 65411);
        ret = bind(con->sockfd, (sockaddr *)&server_new_addr, sizeof(server_new_addr));
    } while (ret == -1); //repeat if the port was already assigned

    con->servaddr = client_addr;
    con->conn_id = conn_id;

    poli_synack synack;
    synack.hdr.protocol_id = POLI_PROTOCOL_ID;
    synack.hdr.conn_id = conn_id;
    synack.hdr.type = SYNACK;
    synack.hdr.ack_num = 0;
    synack.hdr.recv_window = 0;
    synack.assigned_port = server_new_addr.sin_port;

    while (1) {
        DEBUG_PRINT("Sending SYN+ACK communicating assigned port is: %d\n", ntohs(synack.assigned_port));
        sendto(listen_sockfd, &synack, sizeof(synack), 0, (sockaddr *)&client_addr, client_addr_len);

        int bytes_recvd = recvfrom(con->sockfd, buf, sizeof(buf), 0, NULL, NULL);

        if (bytes_recvd < 0) {
            DEBUG_PRINT("Handshake timeout, resending SYN+ACK...\n");
            continue;
        }

        auto *ack = (poli_tcp_ctrl_hdr *)buf;
        if (ack->protocol_id == POLI_PROTOCOL_ID && ack->type == ACK) {
            DEBUG_PRINT("Recieved ACK from: %s: %d\n", inet_ntoa(con->servaddr.sin_addr), ntohs(con->servaddr.sin_port));
            break;
        } else {
            //Recieved something else
            DEBUG_PRINT("Received non-ACK, resending SYN+ACK\n");
        }
    }

    pthread_mutex_init(&con->con_lock, NULL);
    cons.insert({conn_id, con});

    /* Since we can have multiple connection, we want to know if data is available
       on the socket used by a given connection. We use POLL for this */
    data_fds[fdmax].fd = con->sockfd;    
    data_fds[fdmax].events = POLLIN;    
    
    /* This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on a connection */
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME,  0);    
    timer_fds[fdmax].events = POLLIN;    
    struct itimerspec spec;     
    spec.it_value.tv_sec = 0;
    spec.it_value.tv_nsec = 100000000;
    spec.it_interval.tv_sec = 0;
    spec.it_interval.tv_nsec = 100000000;
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);    
    fdmax++;

    DEBUG_PRINT("Connection established!\n");

    return conn_id;
}

void init_receiver(int recv_buffer_bytes)
{
    pthread_t thread1;
    int ret;

    /* TODO: Create the connection socket and bind it to 8032 */
    listen_sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (listen_sockfd == -1) {
        DEBUG_PRINT("Couldn't create socket\n");
        exit(-1);
    }

    sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(8032);

    int bind_ret = bind(listen_sockfd, (sockaddr *)&addr, sizeof(addr));
    if (bind_ret == -1) {
        DEBUG_PRINT("Couldn't bind socket\n");
        exit(-1);
    }

    ret = pthread_create( &thread1, NULL, receiver_handler, NULL);
    assert(ret == 0);
}
