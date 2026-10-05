#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

int main() {
    int client = socket(AF_INET, SOCK_STREAM, 0);

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(5000);
    inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr);

    connect(client, (sockaddr*)&server_addr,
            sizeof(server_addr));

    const char* message = "Hello, server!";
    send(client, message, strlen(message), 0);

    char buffer[100] = {};
    recv(client, buffer, sizeof(buffer), 0);

    std::cout << "Server: " << buffer << '\n';

    close(client);

    return 0;
}