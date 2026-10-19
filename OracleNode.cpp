#include <bits/stdc++.h>
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <cerrno>
#include <filesystem>

using namespace std;
namespace fs = std::filesystem;

string config_file;
vector<vector<int>> adj_matrix;
int expected_nodes = 0;
bool config_read_ok = false;

const int TCP_PORT = 5000;
const int CONNECT_BODY_SIZE = 6;
const int MAX_NODES = 26;

struct VirtualNode {
    int tcp_socket = -1;
    bool assigned = false;
    bool registered = false;
    bool ever_registered = false;
    string ip_address;
    uint16_t udp_port = 0;
    vector<unsigned char> input_buffer;
};

vector<VirtualNode> nodes;

void readConfig(string filename, vector<vector<int>>& result) {
    ifstream file(filename);
    vector<vector<int>> rows;
    string line;

    while (getline(file, line)) {
        stringstream input(line);
        string first;
        if (!(input >> first) || first[0] == '#') continue;
        stringstream values(line);
        vector<int> row;
        int cost;
        while (values >> cost) row.push_back(cost);
        rows.push_back(row);
    }

    int n = static_cast<int>(rows.size()) + 1;
    vector<vector<int>> candidate(n, vector<int>(n, -1));
    for (int i = 0; i < n; ++i) candidate[i][i] = 0;
    for (int i = 0; i < n - 1; ++i)
        for (int j = i + 1; j < n; ++j)
            candidate[i][j] = candidate[j][i] = rows[i][j - i - 1];

    result = std::move(candidate);
    if (expected_nodes == 0) expected_nodes = n;
    config_read_ok = true;
}

bool getReachableIPv4(string& ip_text) {
    const char* override_ip = getenv("ORACLE_BIND_IP");
    if (override_ip && *override_ip) { ip_text = override_ip; return true; }

    int probe = socket(AF_INET, SOCK_DGRAM, 0);
    if (probe >= 0) {
        sockaddr_in destination{};
        destination.sin_family = AF_INET;
        destination.sin_port = htons(9);
        inet_pton(AF_INET, "192.0.2.1", &destination.sin_addr);
        if (connect(probe, reinterpret_cast<sockaddr*>(&destination), sizeof(destination)) == 0) {
            sockaddr_in local{};
            socklen_t len = sizeof(local);
            if (getsockname(probe, reinterpret_cast<sockaddr*>(&local), &len) == 0) {
                char buffer[INET_ADDRSTRLEN]{};
                inet_ntop(AF_INET, &local.sin_addr, buffer, sizeof(buffer));
                ip_text = buffer;
                close(probe);
                return true;
            }
        }
        close(probe);
    }

    ifaddrs* interfaces = nullptr;
    if (getifaddrs(&interfaces) != 0) return false;
    for (ifaddrs* item = interfaces; item; item = item->ifa_next) {
        if (item->ifa_addr && item->ifa_addr->sa_family == AF_INET &&
            (item->ifa_flags & IFF_UP) && !(item->ifa_flags & IFF_LOOPBACK)) {
            auto* address = reinterpret_cast<sockaddr_in*>(item->ifa_addr);
            char buffer[INET_ADDRSTRLEN]{};
            inet_ntop(AF_INET, &address->sin_addr, buffer, sizeof(buffer));
            ip_text = buffer;
            freeifaddrs(interfaces);
            return true;
        }
    }
    freeifaddrs(interfaces);
    return false;
}

bool sendAll(int sock, const unsigned char* data, size_t length) {
    size_t sent = 0;
    while (sent < length) {
        ssize_t n = send(sock, data + sent, length - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (n == 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

void closeNodeSocket(int index) {
    if (index < 0 || index >= static_cast<int>(nodes.size())) return;
    if (nodes[index].tcp_socket != -1) {
        close(nodes[index].tcp_socket);
        nodes[index].tcp_socket = -1;
    }
    nodes[index].registered = false;
    nodes[index].input_buffer.clear();
}

bool allExpectedNodesHaveSentConnect() {
    if (nodes.empty()) return false;
    for (const VirtualNode& node : nodes) {
        if (!node.ever_registered) return false;
    }
    return true;
}

bool decodeConnect(int index) {
    VirtualNode& node = nodes[index];
    in_addr address{};
    memcpy(&address.s_addr, node.input_buffer.data(), 4);
    uint16_t network_port;
    memcpy(&network_port, node.input_buffer.data() + 4, 2);
    char ip_buffer[INET_ADDRSTRLEN]{};
    inet_ntop(AF_INET, &address, ip_buffer, sizeof(ip_buffer));
    node.ip_address = ip_buffer;
    node.udp_port = ntohs(network_port);
    node.registered = node.ever_registered = true;
    node.input_buffer.clear();
    cout << "Registered VN " << char('A' + index) << " at "
         << node.ip_address << ':' << node.udp_port << '\n';
    return true;
}

bool sendLinkStateToNode(int index) {
    VirtualNode& destination = nodes[index];
    struct Tuple { int node_id; uint32_t address_network_order; uint16_t port_network_order; int32_t cost; };
    vector<Tuple> tuples;
    int n = static_cast<int>(adj_matrix.size());

    for (int j = 0; j < n; ++j) {
        int cost;
        if (j == index) cost = 0;
        else if (adj_matrix[index][j] != -1) cost = adj_matrix[index][j];
        else continue;
        in_addr address{};
        inet_pton(AF_INET, nodes[j].ip_address.c_str(), &address);
        tuples.push_back({j, address.s_addr, htons(nodes[j].udp_port), cost});
    }

    vector<unsigned char> message;
    message.reserve(1 + tuples.size() * 11);
    message.push_back(static_cast<unsigned char>(tuples.size()));
    for (const Tuple& tuple : tuples) {
        message.push_back(static_cast<unsigned char>('A' + tuple.node_id));
        const auto* ip_bytes = reinterpret_cast<const unsigned char*>(&tuple.address_network_order);
        message.insert(message.end(), ip_bytes, ip_bytes + 4);
        const auto* port_bytes = reinterpret_cast<const unsigned char*>(&tuple.port_network_order);
        message.insert(message.end(), port_bytes, port_bytes + 2);
        uint32_t network_cost = htonl(static_cast<uint32_t>(tuple.cost));
        const auto* cost_bytes = reinterpret_cast<const unsigned char*>(&network_cost);
        message.insert(message.end(), cost_bytes, cost_bytes + 4);
    }

    if (!sendAll(destination.tcp_socket, message.data(), message.size())) {
        closeNodeSocket(index);
        return false;
    }
    cout << "Sent LINK-STATE to " << char('A' + index) << '\n';
    return true;
}

void broadcastLinkState() {
    cout << "Broadcasting current topology to connected VNs.\n";
    for (int i = 0; i < static_cast<int>(nodes.size()); ++i) {
        if (nodes[i].tcp_socket != -1 && nodes[i].registered) {
            sendLinkStateToNode(i);
        }
    }
}

void acceptVirtualNode(int listen_socket) {
    sockaddr_in peer_address{};
    socklen_t peer_length = sizeof(peer_address);
    int client_socket = accept(
        listen_socket,
        reinterpret_cast<sockaddr*>(&peer_address),
        &peer_length
    );

    if (client_socket < 0) {
        if (errno != EINTR) perror("accept");
        return;
    }

    int index = -1;
    for (int i = 0; i < static_cast<int>(nodes.size()); ++i) {
        if (!nodes[i].assigned) {
            index = i;
            break;
        }
    }

    if (index == -1) {
        char peer_ip[INET_ADDRSTRLEN]{};
        inet_ntop(AF_INET, &peer_address.sin_addr, peer_ip, sizeof(peer_ip));
        cout << "Extra connection from " << peer_ip
             << " is outside the configured topology; closing it.\n";
        close(client_socket);
        return;
    }

    nodes[index].assigned = true;
    nodes[index].tcp_socket = client_socket;
    nodes[index].registered = false;
    nodes[index].input_buffer.clear();

    char peer_ip[INET_ADDRSTRLEN]{};
    inet_ntop(AF_INET, &peer_address.sin_addr, peer_ip, sizeof(peer_ip));
    cout << "Accepted TCP connection for VN " << char('A' + index)
         << " from " << peer_ip << ':' << ntohs(peer_address.sin_port) << '\n';
}

void handleNodeReadable(int index) {
    VirtualNode& node = nodes[index];
    if (node.tcp_socket == -1) return;

    unsigned char buffer[CONNECT_BODY_SIZE];
    size_t remaining = CONNECT_BODY_SIZE - node.input_buffer.size();
    if (node.registered) {

        remaining = sizeof(buffer);
    }

    ssize_t received = recv(node.tcp_socket, buffer, remaining, 0);
    if (received == 0) {
        cout << "VN " << char('A' + index) << " disconnected.\n";
        closeNodeSocket(index);
        return;
    }
    if (received < 0) {
        if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
            return;
        }
        perror("recv");
        closeNodeSocket(index);
        return;
    }

    if (node.registered) {
        cerr << "Unexpected application-layer data from VN "
             << char('A' + index) << "; closing connection.\n";
        closeNodeSocket(index);
        return;
    }

    node.input_buffer.insert(node.input_buffer.end(), buffer, buffer + received);
    if (node.input_buffer.size() == CONNECT_BODY_SIZE) {
        if (!decodeConnect(index)) {
            closeNodeSocket(index);
        }
    }
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        cerr << "Usage: " << argv[0] << " <config-file>\n";
        return 1;
    }

    config_file = argv[1];
    readConfig(config_file, adj_matrix);

    const int number_of_nodes = static_cast<int>(adj_matrix.size());
    nodes.resize(number_of_nodes);

    cout << "Loaded topology with " << number_of_nodes << " nodes.\n";
    for (int i = 0; i < number_of_nodes; ++i) {
        cout << char('A' + i) << ": ";
        for (int j = 0; j < number_of_nodes; ++j) {
            if (j) cout << '\t';
            cout << adj_matrix[i][j];
        }
        cout << '\n';
    }

    string bind_ip;
    if (!getReachableIPv4(bind_ip)) {
        return 1;
    }

    int listen_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_socket < 0) {
        perror("socket");
        return 1;
    }

    int reuse = 1;
    if (setsockopt(listen_socket, SOL_SOCKET, SO_REUSEADDR,
                   &reuse, sizeof(reuse)) < 0) {
        perror("setsockopt(SO_REUSEADDR)");
        close(listen_socket);
        return 1;
    }

    sockaddr_in server_address{};
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(TCP_PORT);
    inet_pton(AF_INET, bind_ip.c_str(), &server_address.sin_addr);

    if (bind(listen_socket, reinterpret_cast<sockaddr*>(&server_address),
             sizeof(server_address)) < 0) {
        perror("bind");
        close(listen_socket);
        return 1;
    }

    if (listen(listen_socket, 32) < 0) {
        perror("listen");
        close(listen_socket);
        return 1;
    }

    cout << "Oracle Node listening on " << bind_ip << ':' << TCP_PORT
         << " (TCP).\n";
    cout << "VN connection order maps to A, B, C, ...\n";
    cout << "Set ORACLE_BIND_IP if automatic interface detection selects the wrong IP.\n";

    error_code time_error;
    fs::file_time_type last_config_time = fs::last_write_time(config_file, time_error);

    bool topology_dirty = true;

    while (true) {
        fd_set read_set;
        FD_ZERO(&read_set);
        FD_SET(listen_socket, &read_set);
        int max_fd = listen_socket;

        for (const VirtualNode& node : nodes) {
            if (node.tcp_socket != -1) {
                FD_SET(node.tcp_socket, &read_set);
                max_fd = max(max_fd, node.tcp_socket);
            }
        }

        timeval timeout{};
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;

        int ready = select(max_fd + 1, &read_set, nullptr, nullptr, &timeout);
        if (ready < 0) {
            if (errno == EINTR) continue;
            perror("select");
            usleep(500000);
            continue;
        }

        if (FD_ISSET(listen_socket, &read_set)) acceptVirtualNode(listen_socket);
        for (int i = 0; i < static_cast<int>(nodes.size()); ++i) {
            int sock = nodes[i].tcp_socket;
            if (sock != -1 && FD_ISSET(sock, &read_set)) handleNodeReadable(i);
        }

        error_code current_time_error;
        fs::file_time_type current_time =
            fs::last_write_time(config_file, current_time_error);
        if (!current_time_error && current_time != last_config_time) {
            cout << "Detected configuration file modification; reloading.\n";
            readConfig(config_file, adj_matrix);

            last_config_time = current_time;
            topology_dirty = true;
        }
        if (topology_dirty && allExpectedNodesHaveSentConnect()) {
            broadcastLinkState();
            topology_dirty = false;
        }
    }

    for (int i = 0; i < static_cast<int>(nodes.size()); ++i) closeNodeSocket(i);
    close(listen_socket);
    return 0;
}
