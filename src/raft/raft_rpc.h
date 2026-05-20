#pragma once
#include "proto/raft.pb.h"
#include "raft_node.h"
#include <string>
#include <vector>
#include <functional>
#include <memory>
#include <atomic>
#include <thread>
#include <unordered_map>

namespace mini_storage {

class EventLoop;
class TcpServer;
class TcpConnection;

// RaftRPC handles networking for Raft consensus.
// Each HANameNode has one RaftRPC instance that:
//   - Listens on a port for incoming Raft messages
//   - Sends Raft messages to peers (connect/send/close per message)
class RaftRPC {
public:
    RaftRPC(RaftNode* raft_node, const std::string& host, int port);
    ~RaftRPC();

    bool Start();
    void Stop();

    // Send a RaftMessage to a specific peer (peer_id format: "host:port")
    bool SendMessage(const std::string& peer_id, const RaftMessage& msg);

private:
    void OnMessage(std::shared_ptr<TcpConnection> conn, const std::string& data);
    void HandleIncoming(const RaftMessage& msg);

    RaftNode*    raft_node_;
    std::string  host_;
    int          port_;

    std::unique_ptr<EventLoop>  loop_;
    std::unique_ptr<TcpServer>  server_;
    std::thread                 loop_thread_;
    std::atomic<bool>           stop_{false};
};

}  // namespace mini_storage
