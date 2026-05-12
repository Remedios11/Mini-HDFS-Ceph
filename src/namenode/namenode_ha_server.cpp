#include "namenode_ha_server.h"
#include "net_tcp_connection.h"
#include "net_io_helpers.h"
#include <iostream>
#include <chrono>

namespace fs = std::filesystem;

namespace mini_storage {

HANameNodeServer::HANameNodeServer(const std::string& data_dir,
                                     const std::string& client_host, int client_port,
                                     const std::string& raft_host, int raft_port,
                                     const std::vector<std::string>& cluster_peers,
                                     int num_workers)
    : data_dir_(data_dir)
    , cluster_peers_(cluster_peers)
{
    my_peer_id_ = raft_host + ":" + std::to_string(raft_port);
    fs::create_directories(data_dir_);

    // Phase 2 components
    metadata_       = std::make_unique<MetadataStore>();
    block_manager_  = std::make_unique<BlockManager>(metadata_.get());
    dn_manager_     = std::make_unique<DataNodeManager>();
    fault_detector_ = std::make_unique<FaultDetector>(metadata_.get(),
                                                       dn_manager_.get(), 15);

    // Raft
    RaftConfig raft_config;
    raft_config.node_id   = my_peer_id_;
    raft_config.peers     = cluster_peers_;
    raft_config.data_dir  = data_dir_ + "/raft";
    raft_config.election_timeout_ms   = 2000;
    raft_config.heartbeat_interval_ms = 500;
    raft_node_ = std::make_unique<RaftNode>(raft_config);
    raft_rpc_  = std::make_unique<RaftRPC>(raft_node_.get(), raft_host, raft_port);

    // Wire Raft callbacks
    raft_node_->SetApplyCallback([this](uint64_t idx, const std::string& cmd) {
        OnApplyCommitted(idx, cmd);
    });
    raft_node_->SetSendRPCCallback([this](const std::string& peer, const RaftMessage& msg) {
        OnSendRaftRPC(peer, msg);
    });
    raft_node_->SetSnapshotCallback([this]() {
        return TakeMetadataSnapshot();
    });
    raft_node_->SetRestoreCallback([this](const std::string& data) {
        return RestoreMetadataSnapshot(data);
    });

    // Client-facing server
    client_loop_   = std::make_unique<EventLoop>();
    client_server_ = std::make_unique<TcpServer>(client_loop_.get(), client_host, client_port);
    thread_pool_   = std::make_unique<ThreadPool>(num_workers);

    client_server_->SetMessageCallback([this](auto conn, const auto& data) {
        OnClientMessage(conn, data);
    });
}

HANameNodeServer::~HANameNodeServer() { Stop(); }

bool HANameNodeServer::Start() {
    // Start Raft
    if (!raft_node_->Start()) {
        std::cerr << "[HANameNode] Failed to start Raft\n";
        return false;
    }
    if (!raft_rpc_->Start()) {
        std::cerr << "[HANameNode] Failed to start RaftRPC\n";
        return false;
    }

    // Start client server
    if (!client_server_->Start()) {
        std::cerr << "[HANameNode] Failed to start client server\n";
        return false;
    }
    client_loop_thread_ = std::thread([this] { client_loop_->Loop(); });

    // Start health check and fault detector
    StartHealthCheckTimer();
    fault_detector_->Start();

    std::cout << "[HANameNode] Started successfully" << std::endl;
    return true;
}

void HANameNodeServer::Stop() {
    stop_.store(true);
    fault_detector_->Stop();
    if (health_check_thread_.joinable()) health_check_thread_.join();
    raft_node_->Stop();
    raft_rpc_->Stop();
    thread_pool_->Stop();
    client_loop_->Quit();
    if (client_loop_thread_.joinable()) client_loop_thread_.join();
}

bool HANameNodeServer::IsLeader() {
    return raft_node_->IsLeader();
}

// ===== Client Message Handling =====

void HANameNodeServer::OnClientMessage(std::shared_ptr<TcpConnection> conn,
                                         const std::string& data) {
    thread_pool_->Submit([this, conn, data] {
        ProcessClientRequest(conn, data);
    });
}

void HANameNodeServer::ProcessClientRequest(std::shared_ptr<TcpConnection> conn,
                                              const std::string& data) {
    NameNodeRequest req;
    if (!req.ParseFromString(data)) {
        std::cerr << "[HANameNode] Failed to parse request\n";
        return;
    }

    NameNodeResponse resp;

    switch (req.op()) {
    case NameNodeRequest::CREATE_FILE:
    case NameNodeRequest::DELETE_FILE:
    case NameNodeRequest::ALLOCATE_BLOCK:
        // Writes go through Raft
        resp = ProposeWrite(req);
        break;

    case NameNodeRequest::GET_FILE_BLOCKS:
        resp = HandleGetFileBlocks(req.get_file_blocks());
        break;
    case NameNodeRequest::LIST_FILES:
        resp = HandleListFiles(req.list_files());
        break;
    case NameNodeRequest::REGISTER_DN:
        resp = HandleRegisterDN(req.register_dn());
        break;
    case NameNodeRequest::HEARTBEAT:
        resp = HandleHeartbeat(req.heartbeat());
        break;
    case NameNodeRequest::BLOCK_REPORT:
        resp = HandleBlockReport(req.block_report());
        break;
    default:
        resp.set_success(false);
        resp.set_error("Unknown operation");
        break;
    }

    resp.set_request_id(req.request_id());

    std::string resp_bytes;
    resp.SerializeToString(&resp_bytes);
    client_loop_->RunInLoop([conn, resp_bytes] {
        conn->Send(resp_bytes);
    });
}

// ===== Propose Write through Raft =====

NameNodeResponse HANameNodeServer::ProposeWrite(const NameNodeRequest& req,
                                                  int timeout_ms) {
    NameNodeResponse resp;
    resp.set_request_id(req.request_id());

    if (!raft_node_->IsLeader()) {
        std::string leader = raft_node_->GetLeaderID();
        resp.set_success(false);
        resp.set_error("Not leader. Leader: " + leader);
        return resp;
    }

    // Serialize the command
    std::string command;
    if (!req.SerializeToString(&command)) {
        resp.set_success(false);
        resp.set_error("Failed to serialize command");
        return resp;
    }

    // Propose to Raft
    auto result = raft_node_->Propose(command, timeout_ms);

    if (!result.is_leader) {
        resp.set_success(false);
        resp.set_error("Not leader anymore. Leader: " + result.leader_id);
        return resp;
    }

    if (!result.committed) {
        resp.set_success(false);
        resp.set_error("Request timed out (not committed)");
        return resp;
    }

    // By now, the apply callback has been called and MetadataStore updated.
    // Build the response based on the operation type.
    switch (req.op()) {
    case NameNodeRequest::CREATE_FILE: {
        auto file = metadata_->GetFile(req.create_file().path());
        resp.set_success(file.has_value());
        if (!file.has_value())
            resp.set_error("Failed to create file: " + req.create_file().path());
        break;
    }
    case NameNodeRequest::DELETE_FILE: {
        auto file = metadata_->GetFile(req.delete_file().path());
        resp.set_success(!file.has_value());
        if (file.has_value())
            resp.set_error("Failed to delete file: " + req.delete_file().path());
        break;
    }
    case NameNodeRequest::ALLOCATE_BLOCK: {
        auto f = metadata_->GetFile(req.allocate_block().file_path());
        if (f.has_value() && !f->blocks.empty()) {
            BlockID last_bid = f->blocks.back();
            auto block = metadata_->GetBlock(last_bid);
            auto* ar = resp.mutable_allocate_block();
            ar->set_success(true);
            ar->set_block_id(last_bid);
            if (block.has_value()) {
                for (const auto& dn : block->locations)
                    ar->add_datanode_addresses(dn);
            }
            resp.set_success(true);
        } else {
            resp.set_success(false);
            resp.set_error("Block allocation failed");
        }
        break;
    }
    default:
        resp.set_success(false);
        resp.set_error("Unknown write operation");
        break;
    }

    return resp;
}

// ===== Raft Callbacks =====

void HANameNodeServer::OnApplyCommitted(uint64_t index, const std::string& command) {
    NameNodeRequest req;
    if (!req.ParseFromString(command)) {
        std::cerr << "[HANameNode] Failed to parse committed command at index "
                  << index << std::endl;
        return;
    }

    switch (req.op()) {
    case NameNodeRequest::CREATE_FILE:
        ApplyCreateFile(req.create_file());
        break;
    case NameNodeRequest::DELETE_FILE:
        ApplyDeleteFile(req.delete_file());
        break;
    case NameNodeRequest::ALLOCATE_BLOCK:
        ApplyAllocateBlock(req.allocate_block());
        break;
    default:
        break;
    }
}

void HANameNodeServer::OnSendRaftRPC(const std::string& peer_id,
                                      const RaftMessage& msg) {
    raft_rpc_->SendMessage(peer_id, msg);
}

// ===== Apply Handlers (state machine) =====

void HANameNodeServer::ApplyCreateFile(const CreateFileRequest& req) {
    FileInfo info;
    metadata_->CreateFile(req.path(), &info);
}

void HANameNodeServer::ApplyDeleteFile(const DeleteFileRequest& req) {
    auto f = metadata_->GetFile(req.path());
    if (f.has_value()) {
        for (BlockID bid : f->blocks)
            metadata_->DeleteBlock(bid);
        metadata_->DeleteFile(req.path());
    }
}

void HANameNodeServer::ApplyAllocateBlock(const AllocateBlockRequest& req) {
    auto f = metadata_->GetFile(req.file_path());
    if (!f.has_value()) return;

    auto alive_nodes = dn_manager_->GetAliveDataNodes();
    if (alive_nodes.empty()) return;

    try {
        BlockInfo block = block_manager_->AllocateBlock(alive_nodes);
        block_manager_->CommitBlock(block);
        f->blocks.push_back(block.block_id);
        metadata_->UpdateFile(req.file_path(), *f);
    } catch (const std::exception& e) {
        std::cerr << "[HANameNode] ApplyAllocateBlock failed: " << e.what() << std::endl;
    }
}

// ===== Direct Handlers (reads + ephemeral ops) =====

NameNodeResponse HANameNodeServer::HandleGetFileBlocks(const GetFileBlocksRequest& req) {
    NameNodeResponse resp;
    auto f = metadata_->GetFile(req.path());
    if (!f.has_value()) {
        resp.set_success(false);
        resp.set_error("File not found: " + req.path());
        return resp;
    }
    resp.set_success(true);
    auto* gr = resp.mutable_get_file_blocks();
    gr->set_success(true);
    gr->set_path(f->path);
    gr->set_size(f->size);
    for (BlockID bid : f->blocks) {
        auto block = metadata_->GetBlock(bid);
        auto* bl   = gr->add_blocks();
        bl->set_block_id(bid);
        if (block.has_value()) {
            bl->set_size(block->size);
            for (const auto& dn : block->locations)
                bl->add_datanodes(dn);
        }
    }
    return resp;
}

NameNodeResponse HANameNodeServer::HandleListFiles(const ListFilesRequest& req) {
    NameNodeResponse resp;
    resp.set_success(true);
    auto* lr = resp.mutable_list_files();
    lr->set_success(true);
    for (const auto& f : metadata_->ListFiles(req.dir())) {
        auto* fs = lr->add_files();
        fs->set_path(f.path);
        fs->set_size(f.size);
        fs->set_create_time(f.create_time);
        fs->set_block_count((int32_t)f.blocks.size());
    }
    return resp;
}

NameNodeResponse HANameNodeServer::HandleRegisterDN(const RegisterDataNodeRequest& req) {
    NameNodeResponse resp;
    DataNodeInfo info;
    info.id         = req.datanode_id();
    info.host       = req.host();
    info.port       = req.port();
    info.free_space = req.free_space();
    dn_manager_->RegisterDataNode(info);
    resp.set_success(true);
    return resp;
}

NameNodeResponse HANameNodeServer::HandleHeartbeat(const HeartbeatRequest& req) {
    NameNodeResponse resp;
    bool ok = dn_manager_->HandleHeartbeat(
        req.datanode_id(), req.free_space(), req.block_count());
    resp.set_success(ok);
    if (!ok) resp.set_error("DataNode not registered: " + req.datanode_id());
    return resp;
}

NameNodeResponse HANameNodeServer::HandleBlockReport(const BlockReportRequest& req) {
    NameNodeResponse resp;
    for (const auto& item : req.blocks()) {
        metadata_->UpdateBlockLocation(item.block_id(), req.datanode_id());
        auto existing = metadata_->GetBlock(item.block_id());
        if (existing.has_value() && existing->size == 0 && item.size() > 0) {
            BlockInfo bi = *existing;
            bi.size = item.size();
            metadata_->UpdateBlock(bi);
        }
    }
    resp.set_success(true);
    return resp;
}

// ===== Snapshot =====

std::string HANameNodeServer::TakeMetadataSnapshot() {
    MetadataStoreSnapshot snap;

    // Serialize all files
    auto files = metadata_->ListFiles("");
    for (const auto& f : files) {
        auto* fe = snap.add_files();
        fe->set_path(f.path);
        fe->set_size(f.size);
        fe->set_create_time(f.create_time);
        fe->set_modify_time(f.modify_time);
        for (BlockID bid : f.blocks)
            fe->add_blocks(bid);
    }

    // Serialize all blocks
    // We need to iterate blocks. Since MetadataStore doesn't expose
    // an iterator, we'll collect block IDs from the files.
    for (const auto& f : files) {
        for (BlockID bid : f.blocks) {
            auto block = metadata_->GetBlock(bid);
            if (block.has_value()) {
                auto* be = snap.add_blocks();
                be->set_block_id(block->block_id);
                be->set_size(block->size);
                for (const auto& loc : block->locations)
                    be->add_locations(loc);
            }
        }
    }

    std::string data;
    snap.SerializeToString(&data);
    return data;
}

bool HANameNodeServer::RestoreMetadataSnapshot(const std::string& data) {
    MetadataStoreSnapshot snap;
    if (!snap.ParseFromString(data)) {
        std::cerr << "[HANameNode] Failed to parse snapshot\n";
        return false;
    }

    // Restore blocks first
    for (const auto& be : snap.blocks()) {
        BlockInfo bi;
        bi.block_id = be.block_id();
        bi.size     = be.size();
        for (const auto& loc : be.locations())
            bi.locations.push_back(loc);
        metadata_->AddBlock(bi);
    }

    // Restore files
    for (const auto& fe : snap.files()) {
        FileInfo fi;
        fi.path        = fe.path();
        fi.size        = fe.size();
        fi.create_time = fe.create_time();
        fi.modify_time = fe.modify_time();
        for (uint64_t bid : fe.blocks())
            fi.blocks.push_back(bid);
        // Use internal access - CreateFile won't work if it exists
        // We need direct access. Hack: delete then create.
        metadata_->DeleteFile(fe.path());
        FileInfo unused;
        metadata_->CreateFile(fe.path(), &unused);
        // Update with full info
        metadata_->UpdateFile(fe.path(), fi);
    }

    std::cout << "[HANameNode] Restored snapshot: " << snap.files_size()
              << " files, " << snap.blocks_size() << " blocks\n";
    return true;
}

// ===== Health Check =====

void HANameNodeServer::StartHealthCheckTimer() {
    health_check_thread_ = std::thread([this] {
        while (!stop_.load()) {
            std::this_thread::sleep_for(std::chrono::seconds(5));
            if (!stop_.load()) {
                dn_manager_->CheckDataNodeHealth();
                std::cout << "[HANameNode] DataNodes: "
                          << dn_manager_->AliveCount() << " alive / "
                          << dn_manager_->TotalCount() << " total"
                          << " | Raft: " << (IsLeader() ? "LEADER" : "FOLLOWER")
                          << std::endl;

                // Periodically take snapshot if leader
                if (IsLeader() && raft_node_->GetLogSize() > 50) {
                    raft_node_->TakeSnapshot();
                }
            }
        }
    });
}

}  // namespace mini_storage
