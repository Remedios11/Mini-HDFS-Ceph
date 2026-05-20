#include "net_tcp_connection.h"
#include "net_channel.h"
#include "net_event_loop.h"
#include <unistd.h>
#include <cstring>
#include <arpa/inet.h>
#include <iostream>

namespace mini_storage {

TcpConnection::TcpConnection(EventLoop* loop, int fd)
    : loop_(loop), fd_(fd) {
    channel_ = std::make_unique<Channel>(loop_, fd_);
}

TcpConnection::~TcpConnection() {
    if (fd_ >= 0) {
        channel_->DisableAll();
        close(fd_);
        fd_ = -1;
    }
}

void TcpConnection::Start() {
    channel_->SetReadCallback([this]  { HandleRead();  });
    channel_->SetWriteCallback([this] { HandleWrite(); });
    channel_->SetCloseCallback([this] { HandleClose(); });
    channel_->SetErrorCallback([this] { HandleClose(); });
    channel_->EnableReading();
}

void TcpConnection::HandleRead() {
    char buf[4096];
    while (true) {
        ssize_t n = read(fd_, buf, sizeof(buf));
        if (n > 0) {
            read_buf_.append(buf, n);
            ProcessMessages();
        } else if (n == 0) {
            HandleClose();
            return;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            HandleClose();
            return;
        }
    }
}

void TcpConnection::ProcessMessages() {
    while (read_buf_.size() >= 4) {
        uint32_t msg_len;
        memcpy(&msg_len, read_buf_.data(), 4);
        msg_len = ntohl(msg_len);

        if (read_buf_.size() < 4 + (size_t)msg_len) break;

        std::string message(read_buf_.data() + 4, msg_len);
        read_buf_.erase(0, 4 + msg_len);

        if (msg_cb_) msg_cb_(shared_from_this(), message);
    }
}

void TcpConnection::Send(const std::string& data) {
    uint32_t len = htonl((uint32_t)data.size());
    std::string framed;
    framed.append(reinterpret_cast<char*>(&len), 4);
    framed.append(data);

    if (!write_buf_.empty()) {
        write_buf_.append(framed);
        return;
    }

    ssize_t n = write(fd_, framed.data(), framed.size());
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) n = 0;
        else { HandleClose(); return; }
    }
    if ((size_t)n < framed.size()) {
        write_buf_.append(framed.data() + n, framed.size() - n);
        channel_->EnableWriting();
    }
}

void TcpConnection::HandleWrite() {
    if (write_buf_.empty()) { channel_->DisableWriting(); return; }

    ssize_t n = write(fd_, write_buf_.data(), write_buf_.size());
    if (n > 0) {
        write_buf_.erase(0, n);
        if (write_buf_.empty()) channel_->DisableWriting();
    } else if (n < 0 && errno != EAGAIN) {
        HandleClose();
    }
}

void TcpConnection::HandleClose() {
    channel_->DisableAll();
    if (close_cb_) close_cb_();
}

void TcpConnection::Close() {
    HandleClose();
}

}  // namespace mini_storage
