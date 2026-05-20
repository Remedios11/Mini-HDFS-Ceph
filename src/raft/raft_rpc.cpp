#include "raft_rpc.h"
#include "net_tcp_server.h"
#include "net_tcp_connection.h"
#include "net_event_loop.h"
#include "net_io_helpers.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <iostream>

namespace mini_storage {

RaftRPC::RaftRPC(RaftNode* raft_node, const std::string& host, int port)
    : raft_node_(raft_node), host_(host), port_(port)
{
    loop_   = std::make_unique<EventLoop>();
    server_ = std::make_unique<TcpServer>(loop_.get(), host_, port_);
    server_->SetMessageCallback([this](auto conn, const auto& data) {
        OnMessage(conn, data);
    });
}

RaftRPC::~RaftRPC() { Stop(); }

bool RaftRPC::Start() {
    if (!server_->Start()) {
        std::cerr << "[RaftRPC] Failed to start on " << host_ << ":" << port_ << "\n";
        return false;
    }
    loop_thread_ = std::thread([this] { loop_->Loop(); });
    std::cout << "[RaftRPC] Listening on " << host_ << ":" << port_ << "\n";
    return true;
}

void RaftRPC::Stop() {
    stop_.store(true);
    loop_->Quit();
    if (loop_thread_.joinable()) loop_thread_.join();
}

// OnMessage: called when the TcpServer receives data from a peer.
// We parse the message as a RaftMessage. If it's a request (AppendEntries,
// RequestVote, InstallSnapshot), we process it and send the response back
// through the same connection. If it's a response, we deliver it to RaftNode.
void RaftRPC::OnMessage(std::shared_ptr<TcpConnection> conn, const std::string& data) {
    RaftMessage msg;
    if (!msg.ParseFromString(data)) {
        std::cerr << "[RaftRPC] Failed to parse RaftMessage\n";
        return;
    }

    RaftMessage reply;

    switch (msg.type()) {
    case RaftMessage::REQUEST_VOTE: {
        auto* resp = raft_node_->HandleRequestVote(
            msg.request_vote().candidate_id(), msg.request_vote());
        reply.set_type(RaftMessage::REQUEST_VOTE_RESPONSE);
        *reply.mutable_request_vote_response() = *resp;
        break;
    }
    case RaftMessage::APPEND_ENTRIES: {
        auto* resp = raft_node_->HandleAppendEntries(
            msg.append_entries().leader_id(), msg.append_entries());
        reply.set_type(RaftMessage::APPEND_ENTRIES_RESPONSE);
        *reply.mutable_append_entries_response() = *resp;
        break;
    }
    case RaftMessage::INSTALL_SNAPSHOT: {
        auto* resp = raft_node_->HandleInstallSnapshot(
            msg.install_snapshot().leader_id(), msg.install_snapshot());
        reply.set_type(RaftMessage::INSTALL_SNAPSHOT_RESPONSE);
        *reply.mutable_install_snapshot_response() = *resp;
        break;
    }
    case RaftMessage::REQUEST_VOTE_RESPONSE: {
        raft_node_->HandleRequestVoteResponse(
            "", msg.request_vote_response());
        return;
    }
    case RaftMessage::APPEND_ENTRIES_RESPONSE: {
        raft_node_->HandleAppendEntriesResponse(
            msg.append_entries_response().match_index() > 0 ? "" : "",
            msg.append_entries_response());
        return;
    }
    case RaftMessage::INSTALL_SNAPSHOT_RESPONSE:
        return;
    }

    // Send the response back through the same connection
    std::string reply_bytes;
    if (reply.SerializeToString(&reply_bytes)) {
        conn->Send(reply_bytes);
    }
}

// SendMessage: send a RaftMessage to a peer. Used for both requests and responses.
// For outgoing requests (RequestVote, AppendEntries, InstallSnapshot),
// we connect, send, read the response, and deliver it back.
bool RaftRPC::SendMessage(const std::string& peer_id, const RaftMessage& msg) {
    size_t colon = peer_id.rfind(':');
    if (colon == std::string::npos) return false;

    std::string peer_host = peer_id.substr(0, colon);
    int         peer_port = std::stoi(peer_id.substr(colon + 1));

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;

    struct timeval tv{3, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(peer_port);
    inet_pton(AF_INET, peer_host.c_str(), &addr.sin_addr);

    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(fd);
        return false;
    }

    std::string bytes;
    if (!msg.SerializeToString(&bytes)) { close(fd); return false; }

    if (!SendMsg(fd, bytes)) { close(fd); return false; }

    // Read response for requests that expect one
    auto msg_type = msg.type();
    if (msg_type == RaftMessage::APPEND_ENTRIES ||
        msg_type == RaftMessage::REQUEST_VOTE ||
        msg_type == RaftMessage::INSTALL_SNAPSHOT) {

        std::string resp_bytes;
        if (RecvMsg(fd, &resp_bytes)) {
            RaftMessage resp_msg;
            if (resp_msg.ParseFromString(resp_bytes)) {
                // Deliver response to RaftNode
                switch (resp_msg.type()) {
                case RaftMessage::REQUEST_VOTE_RESPONSE:
                    raft_node_->HandleRequestVoteResponse(
                        peer_id, resp_msg.request_vote_response());
                    break;
                case RaftMessage::APPEND_ENTRIES_RESPONSE:
                    raft_node_->HandleAppendEntriesResponse(
                        peer_id, resp_msg.append_entries_response());
                    break;
                case RaftMessage::INSTALL_SNAPSHOT_RESPONSE:
                    // Snap response - currently no special handling needed
                    break;
                default: break;
                }
            }
        }
    }

    close(fd);
    return true;
}

}  // namespace mini_storage
