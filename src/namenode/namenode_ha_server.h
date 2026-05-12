#pragma once
#include "namenode_metadata_store.h"
#include "namenode_block_manager.h"
#include "namenode_datanode_manager.h"
#include "namenode_fault_detector.h"
#include "raft/raft_node.h"
#include "raft/raft_rpc.h"
#include "net_tcp_server.h"
#include "net_event_loop.h"
#include "net_thread_pool.h"
#include "proto/namenode.pb.h"
#include "proto/raft.pb.h"
#include <memory>
#include <thread>
#include <atomic>
#include <string>
#include <filesystem>

namespace mini_storage {

class TcpConnection;

// HANameNodeServer: High-Availability NameNode with Raft consensus.
// Replaces NameNodeServer in Phase 3 deployments.
// Creates a Raft cluster among multiple NameNodes for leader election
// and state replication.
class HANameNodeServer {
public:
    // cluster_peers: list of all NameNode addresses ("host:raft_port")
    // my_peer_id: which one of those is me
    // client_host/client_port: the existing NameNode client service port
    // raft_port: port for Raft inter-node communication
    HANameNodeServer(const std::string& data_dir,
                     const std::string& client_host, int client_port,
                     const std::string& raft_host, int raft_port,
                     const std::vector<std::string>& cluster_peers,
                     int num_workers = 4);
    ~HANameNodeServer();

    bool Start();
    void Stop();

    // Accessors for tests
    MetadataStore*   GetMetadataStore()   { return metadata_.get(); }
    DataNodeManager* GetDataNodeManager() { return dn_manager_.get(); }
    FaultDetector*   GetFaultDetector()   { return fault_detector_.get(); }
    RaftNode*        GetRaftNode()        { return raft_node_.get(); }
    bool             IsLeader();

private:
    // Client request handling (same protocol as NameNodeServer)
    void OnClientMessage(std::shared_ptr<TcpConnection> conn, const std::string& data);
    void ProcessClientRequest(std::shared_ptr<TcpConnection> conn, const std::string& data);

    // Raft callbacks
    void OnApplyCommitted(uint64_t index, const std::string& command);
    void OnSendRaftRPC(const std::string& peer_id, const RaftMessage& msg);

    // Snapshot support
    std::string TakeMetadataSnapshot();
    bool RestoreMetadataSnapshot(const std::string& data);

    // Propose a write operation through Raft
    NameNodeResponse ProposeWrite(const NameNodeRequest& req, int timeout_ms = 5000);

    // Direct handlers (for reads and ephemeral ops)
    NameNodeResponse HandleGetFileBlocks(const GetFileBlocksRequest& req);
    NameNodeResponse HandleListFiles(const ListFilesRequest& req);
    NameNodeResponse HandleRegisterDN(const RegisterDataNodeRequest& req);
    NameNodeResponse HandleHeartbeat(const HeartbeatRequest& req);
    NameNodeResponse HandleBlockReport(const BlockReportRequest& req);

    // Apply handlers (called when Raft commits a log entry)
    void ApplyCreateFile(const CreateFileRequest& req);
    void ApplyDeleteFile(const DeleteFileRequest& req);
    void ApplyAllocateBlock(const AllocateBlockRequest& req);

    // Health check
    void StartHealthCheckTimer();

    // Data directory
    std::string data_dir_;

    // Phase 2 components (same as NameNodeServer)
    std::unique_ptr<MetadataStore>    metadata_;
    std::unique_ptr<BlockManager>     block_manager_;
    std::unique_ptr<DataNodeManager>  dn_manager_;
    std::unique_ptr<FaultDetector>    fault_detector_;

    // Raft components
    std::unique_ptr<RaftNode>  raft_node_;
    std::unique_ptr<RaftRPC>   raft_rpc_;

    // Client-facing network (Phase 2 protocol)
    std::unique_ptr<EventLoop>  client_loop_;
    std::unique_ptr<TcpServer>  client_server_;
    std::unique_ptr<ThreadPool> thread_pool_;
    std::thread                 client_loop_thread_;

    // Health check
    std::thread        health_check_thread_;
    std::atomic<bool>  stop_{false};

    // Cluster config
    std::string              my_peer_id_;      // "raft_host:raft_port"
    std::vector<std::string> cluster_peers_;

    // Next block ID counter (recovered from state)
    std::atomic<uint64_t> next_block_id_{1};

    // For propose-to-apply result passing
    std::mutex              propose_result_mutex_;
    std::condition_variable propose_result_cv_;
    std::unordered_map<uint64_t, NameNodeResponse> propose_results_;
};

}  // namespace mini_storage
