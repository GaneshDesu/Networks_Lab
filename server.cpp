#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

int main() {
    int server = socket(AF_INET, SOCK_STREAM, 0);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(5000);
    addr.sin_addr.s_addr = INADDR_ANY;

    bind(server, (sockaddr*)&addr, sizeof(addr));
    listen(server, 5);

    std::cout << "Waiting for client...\n";

    sockaddr_in client_addr{};
    socklen_t len = sizeof(client_addr);
    int client = accept(server, (sockaddr*)&client_addr, &len);

    char buffer[100] = {};

    recv(client, buffer, sizeof(buffer), 0);
    std::cout << "Client: " << buffer << '\n';

    const char* reply = "Hi, client!";
    send(client, reply, strlen(reply), 0);

    close(client);
    close(server);

    return 0;
}