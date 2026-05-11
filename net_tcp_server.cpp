#include "net_tcp_server.h"
#include "net_event_loop.h"
#include "net_channel.h"
#include "net_tcp_connection.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <iostream>
#include <stdexcept>

namespace mini_storage {

static void SetNonBlocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

TcpServer::TcpServer(EventLoop* loop, const std::string& host, int port)
    : loop_(loop), host_(host), port_(port) {}

TcpServer::~TcpServer() {
    if (server_fd_ >= 0) close(server_fd_);
}

bool TcpServer::Start() {
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0) return false;

    int opt = 1;
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port_);
    inet_pton(AF_INET, host_.c_str(), &addr.sin_addr);

    if (bind(server_fd_, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "[TcpServer] bind failed: " << strerror(errno) << std::endl;
        return false;
    }

    if (listen(server_fd_, 128) < 0) return false;

    SetNonBlocking(server_fd_);
    accept_channel_ = std::make_unique<Channel>(loop_, server_fd_);
    accept_channel_->SetReadCallback([this] { HandleNewConnection(); });
    accept_channel_->EnableReading();

    std::cout << "[TcpServer] listening on " << host_ << ":" << port_ << std::endl;
    return true;
}

void TcpServer::HandleNewConnection() {
    sockaddr_in client_addr{};
    socklen_t   len = sizeof(client_addr);
    int client_fd = accept(server_fd_, (sockaddr*)&client_addr, &len);
    if (client_fd < 0) return;

    SetNonBlocking(client_fd);

    auto conn = std::make_shared<TcpConnection>(loop_, client_fd);
    connections_[client_fd] = conn;

    conn->SetMessageCallback(msg_cb_);
    conn->SetCloseCallback([this, client_fd]() {
        connections_.erase(client_fd);
    });
    conn->Start();

    if (conn_cb_) conn_cb_(conn);
}

}  // namespace mini_storage
