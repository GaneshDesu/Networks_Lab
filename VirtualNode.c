#include <arpa/inet.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define ORACLE_PORT  5000
#define MAX_NODES    26
#define TUPLE_LEN    11
#define HDR_LEN      10
#define MAX_LSP_LEN  (HDR_LEN + MAX_NODES * TUPLE_LEN)
#define MAX_MSG_LEN  (1 + MAX_NODES * TUPLE_LEN)
#define COPIES       3
#define REFRESH_MS   15000
#define PRINT_MS     15000

struct link {
    char id;
    struct in_addr ip;
    uint16_t port;
    int32_t cost;
};

struct lsp {
    int valid;
    char origin;
    int64_t seq;
    int n;
    struct link links[MAX_NODES];
    int len;
    char raw[MAX_LSP_LEN];
};

struct in_addr oracle_ip, my_ip;
uint16_t my_port;
int udp_fd, tcp_fd;

char my_id = 0;
struct link my_links[MAX_NODES];
int my_link_cnt = 0;
int64_t my_seq = 0;
struct lsp lsdb[MAX_NODES];

int64_t last_lsp_time = 0;
int64_t last_print_time = 0;

char inbuf[MAX_MSG_LEN];
int inlen = 0;

int64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

int get_ip(void) {
    int probe = socket(AF_INET, SOCK_DGRAM, 0);
    if (probe < 0) return 0;

    struct sockaddr_in destination;
    memset(&destination, 0, sizeof(destination));
    destination.sin_family = AF_INET;
    destination.sin_port = htons(9);
    inet_pton(AF_INET, "192.0.2.1", &destination.sin_addr);

    if (connect(probe, (struct sockaddr *)&destination, sizeof(destination)) == 0) {
        struct sockaddr_in local;
        socklen_t len = sizeof(local);

        if (getsockname(probe, (struct sockaddr *)&local, &len) == 0) {
            my_ip = local.sin_addr;
            close(probe);
            return 1;
        }
    }

    close(probe);
    return 0;
}

void read_tuples(char *p, int n, struct link *out) {
    for (int i = 0; i < n; i++) {
        uint16_t port;
        uint32_t cost;

        out[i].id = p[0];
        memcpy(&out[i].ip.s_addr, p + 1, 4);
        memcpy(&port, p + 5, 2);
        memcpy(&cost, p + 7, 4);
        out[i].port = ntohs(port);
        out[i].cost = (int32_t)ntohl(cost);
        p += TUPLE_LEN;
    }
}

void write_tuples(char *p, struct link *links, int n) {
    for (int i = 0; i < n; i++) {
        uint16_t port = htons(links[i].port);
        uint32_t cost = htonl((uint32_t)links[i].cost);

        p[0] = links[i].id;
        memcpy(p + 1, &links[i].ip.s_addr, 4);
        memcpy(p + 5, &port, 2);
        memcpy(p + 7, &cost, 4);
        p += TUPLE_LEN;
    }
}

int build_lsp(char *pkt, char origin, int64_t seq, struct link *links, int n) {
    pkt[0] = origin;
    for (int i = 0; i < 8; i++)
        pkt[1 + i] = ((uint64_t)seq >> (56 - 8 * i)) & 0xff;
    pkt[9] = n;
    write_tuples(pkt + HDR_LEN, links, n);
    return HDR_LEN + n * TUPLE_LEN;
}

void store_lsp(struct lsp *r, char origin, int64_t seq, struct link *links,
               int n, char *raw, int len) {
    r->valid = 1;
    r->origin = origin;
    r->seq = seq;
    r->n = n;
    memcpy(r->links, links, n * sizeof(struct link));
    r->len = len;
    memcpy(r->raw, raw, len);
}

void flood(char *pkt, int len, struct in_addr *skip_ip, uint16_t skip_port) {
    for (int i = 0; i < my_link_cnt; i++) {
        struct link *nb = &my_links[i];
        struct sockaddr_in dst;

        if (nb->cost == 0) continue;
        if (skip_ip && nb->ip.s_addr == skip_ip->s_addr && nb->port == skip_port) continue;

        memset(&dst, 0, sizeof dst);
        dst.sin_family = AF_INET;
        dst.sin_addr = nb->ip;
        dst.sin_port = htons(nb->port);

        for (int c = 0; c < COPIES; c++) sendto(udp_fd, pkt, len, 0, (struct sockaddr *)&dst, sizeof dst);
    }
}

void originate_lsp(void) {
    char pkt[MAX_LSP_LEN];
    int64_t seq = ++my_seq;
    int len = build_lsp(pkt, my_id, seq, my_links, my_link_cnt);

    store_lsp(&lsdb[my_id - 'A'], my_id, seq, my_links, my_link_cnt, pkt, len);
    last_lsp_time = now_ms();
    flood(pkt, len, NULL, 0);
}

void got_oracle_state(struct link *links, int n) {
    memcpy(my_links, links, n * sizeof(struct link));
    my_link_cnt = n;

    for (int i = 0; i < n; i++)
        if (links[i].cost == 0) my_id = links[i].id;

    printf("Received LINK-STATE from Oracle Node:\n");
    for (int i = 0; i < n; i++) printf("%s%c=%d", i ? ", " : "", links[i].id, links[i].cost);
    printf("\n");
    originate_lsp();

    for (int i = 0; i < MAX_NODES; i++)
        if (lsdb[i].valid && lsdb[i].origin != my_id)
            flood(lsdb[i].raw, lsdb[i].len, NULL, 0);
}

void read_oracle(void) {
    int n = recv(tcp_fd, inbuf + inlen, sizeof inbuf - inlen, 0);
    if (n <= 0) {
        printf("Oracle Node closed the TCP connection.\n");
        exit(1);
    }
    inlen += n;

    while (inlen > 0) {
        int count = inbuf[0];
        int msg_len = 1 + count * TUPLE_LEN;
        struct link links[MAX_NODES];

        if (inlen < msg_len) break;
        read_tuples(inbuf + 1, count, links);
        got_oracle_state(links, count);

        inlen -= msg_len;
        memmove(inbuf, inbuf + msg_len, inlen);
    }
}

void read_udp(void) {
    char buf[MAX_LSP_LEN + 100];
    struct sockaddr_in from;
    socklen_t flen = sizeof from;
    struct lsp lsp;
    uint64_t seq = 0;
    int len = recvfrom(udp_fd, buf, sizeof buf, 0, (struct sockaddr *)&from, &flen);

    if (len < HDR_LEN) return;
    int n = buf[9];
    if (buf[0] < 'A' || buf[0] > 'Z' || n < 1 || n > MAX_NODES ||
        len != HDR_LEN + n * TUPLE_LEN) return;

    for (int i = 1; i <= 8; i++) seq = (seq << 8) | ((uint64_t)buf[i] & 0xff);

    memset(&lsp, 0, sizeof lsp);
    read_tuples(buf + HDR_LEN, n, lsp.links);
    store_lsp(&lsp, buf[0], (int64_t)seq, lsp.links, n, buf, len);

    if (lsp.origin == my_id) return;

    struct lsp *old = &lsdb[lsp.origin - 'A'];
    if (old->valid && lsp.seq <= old->seq) return;

    *old = lsp;
    printf("Got newer LSP for node %c (sequence %lld).\n", lsp.origin, (long long)lsp.seq);
    flood(lsp.raw, lsp.len, &from.sin_addr, ntohs(from.sin_port));
}

void print_graph(void) {
    printf("\nKnown graph at VN %c:\n", my_id);
    for (int i = 0; i < MAX_NODES; i++) {
        if (!lsdb[i].valid) continue;
        printf("  %c: ", lsdb[i].origin);
        for (int j = 0; j < lsdb[i].n; j++)
            printf("%s%c=%d", j ? ", " : "", lsdb[i].links[j].id, lsdb[i].links[j].cost);
        printf("\n");
    }
    printf("\n");
}

int main(int argc, char *argv[]) {
    struct sockaddr_in addr;
    char msg[6];
    uint16_t net_port;

    if (argc != 3) {
        fprintf(stderr, "Usage: %s <oracle-ipv4> <own-udp-port>\n", argv[0]);
        return 1;
    }

    if (inet_pton(AF_INET, argv[1], &oracle_ip) != 1) {
        fprintf(stderr, "Bad oracle address: %s\n", argv[1]);
        return 1;
    }
    my_port = atoi(argv[2]);

    if (!get_ip()) {
        fprintf(stderr, "Could not determine local IPv4 address.\n");
        return 1;
    }

    udp_fd = socket(AF_INET, SOCK_DGRAM, 0);
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_addr = my_ip;
    addr.sin_port = htons(my_port);
    if (bind(udp_fd, (struct sockaddr *)&addr, sizeof addr) < 0) {
        perror("bind");
        return 1;
    }

    tcp_fd = socket(AF_INET, SOCK_STREAM, 0);
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_addr = oracle_ip;
    addr.sin_port = htons(ORACLE_PORT);
    if (connect(tcp_fd, (struct sockaddr *)&addr, sizeof addr) < 0) {
        perror("connect");
        return 1;
    }

    printf("VN UDP socket bound to %s:%u\n", inet_ntoa(my_ip), my_port);
    printf("Connected to Oracle Node at %s:%d\n", inet_ntoa(oracle_ip), ORACLE_PORT);

    net_port = htons(my_port);
    memcpy(msg, &my_ip.s_addr, 4);
    memcpy(msg + 4, &net_port, 2);
    send(tcp_fd, msg, sizeof msg, 0);
    printf("Sent CONNECT: %s:%u\n", inet_ntoa(my_ip), my_port);

    while(1) {
        fd_set rset;
        struct timeval tv = {
            0,
            500000
        };
        int maxfd = tcp_fd > udp_fd ? tcp_fd : udp_fd;

        FD_ZERO(&rset);
        FD_SET(tcp_fd, &rset);
        FD_SET(udp_fd, &rset);

        if (select(maxfd + 1, &rset, NULL, NULL, &tv) < 0 && errno != EINTR) {
            perror("select");
            break;
        }

        if (FD_ISSET(tcp_fd, &rset)) read_oracle();
        if (FD_ISSET(udp_fd, &rset)) read_udp();

        if (my_id == 0) continue;

        int64_t now = now_ms();
        if (now - last_lsp_time >= REFRESH_MS) {
            printf("Originating LSP for %c (periodic refresh).\n", my_id);
            originate_lsp();
        }
        if (now - last_print_time >= PRINT_MS) {
            print_graph();
            last_print_time = now;
        }
    }

    close(tcp_fd);
    close(udp_fd);
    return 0;
}
