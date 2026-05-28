#include <cstdint>

#pragma once

/* Values used for protocol ID and type. */
#define POLI_PROTOCOL_ID 42

#define SYN 0
#define SYNACK 1
#define ACK 2
#define DATA 3

/* Header for Data segments. Must be used in your implementation as it is. */
struct __attribute__((packed)) poli_tcp_data_hdr {
    uint8_t protocol_id;
    uint8_t conn_id;
    uint8_t type;
    uint16_t seq_num;
    uint16_t len;
};

/* Header for Control segments. Must be used in your implementation as it is. */
struct __attribute__((packed)) poli_tcp_ctrl_hdr {
    uint8_t protocol_id;
    uint8_t conn_id;
    uint8_t type;
    uint16_t ack_num;
    uint16_t recv_window;
};

struct __attribute__((packed)) poli_synack {
    struct poli_tcp_ctrl_hdr hdr;
    uint16_t assigned_port;
};
