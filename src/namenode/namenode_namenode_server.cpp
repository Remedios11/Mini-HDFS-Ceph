#include "namenode_namenode_server.h"
#include "net_tcp_connection.h"
#include "net_io_helpers.h"
#include <iostream>
#include <filesystem>

namespace fs = std::filesystem;

namespace mini_storage {

NameNodeServer::NameNodeServer(const std::string& data_dir,
                                const std::string& host, int port,
                                int num_workers)
    : data_dir_(data_dir), host_(host), port_(port) {
    fs::create_directories(data_dir_);

    metadata_      = std::make_unique<MetadataStore>();
    block_manager_ = std::make_unique<BlockManager>(metadata_.get());
    dn_manager_    = std::make_unique<DataNodeManager>();
    edit_log_      = std::make_unique<EditLog>(data_dir_ + "/edit.log");
    fault_detector_= std::make_unique<FaultDetector>(metadata_.get(),
                                                      dn_manager_.get(), 15);
    loop_          = std::make_unique<EventLoop>();
    thread_pool_   = std::make_unique<ThreadPool>(num_workers);
    server_        = std::make_unique<TcpServer>(loop_.get(), host_, port_);

    // Replay edit log on startup
    edit_log_->Replay([this](const NameNodeRequest& op) {
        // Replay create/allocate only; heartbeats etc. are ephemeral
        if (op.op() == NameNodeRequest::CREATE_FILE) {
            FileInfo info;
            metadata_->CreateFile(op.create_file().path(), &info);
        }
        // Block allocations are rebuilt from DataNode block reports
    });

    server_->SetMessageCallback([this](auto conn, const auto& data) {
        OnMessage(conn, data);
    });
}

NameNodeServer::~NameNodeServer() { Stop(); }

bool NameNodeServer::Start() {
    StartHealthCheckTimer();
    fault_detector_->Start();
    return server_->Start();
}

void NameNodeServer::Run() { loop_->Loop(); }

void NameNodeServer::Stop() {
    stop_.store(true);
    fault_detector_->Stop();
    if (health_check_thread_.joinable()) health_check_thread_.join();
    thread_pool_->Stop();
    loop_->Quit();
}

void NameNodeServer::OnMessage(std::shared_ptr<TcpConnection> conn,
                                const std::string& data) {
    thread_pool_->Submit([this, conn, data] {
        ProcessRequest(conn, data);
    });
}

void NameNodeServer::ProcessRequest(std::shared_ptr<TcpConnection> conn,
                                     const std::string& data) {
    NameNodeRequest req;
    if (!req.ParseFromString(data)) {
        std::cerr << "[NameNode] Failed to parse request\n";
        return;
    }

    NameNodeResponse resp;
    resp.set_request_id(req.request_id());

    switch (req.op()) {
        case NameNodeRequest::CREATE_FILE:
            resp = HandleCreateFile(req.create_file());
            edit_log_->Append(req);
            break;
        case NameNodeRequest::ALLOCATE_BLOCK:
            resp = HandleAllocateBlock(req.allocate_block());
            break;
        case NameNodeRequest::GET_FILE_BLOCKS:
            resp = HandleGetFileBlocks(req.get_file_blocks());
            break;
        case NameNodeRequest::DELETE_FILE:
            resp = HandleDeleteFile(req.delete_file());
            edit_log_->Append(req);
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
    }
    resp.set_request_id(req.request_id());

    std::string resp_bytes;
    resp.SerializeToString(&resp_bytes);
    loop_->RunInLoop([conn, resp_bytes] { conn->Send(resp_bytes); });
}

// ===== Handlers =====

NameNodeResponse NameNodeServer::HandleCreateFile(const CreateFileRequest& req) {
    NameNodeResponse resp;
    FileInfo info;
    if (metadata_->CreateFile(req.path(), &info)) {
        resp.set_success(true);
    } else {
        resp.set_success(false);
        resp.set_error("File already exists: " + req.path());
    }
    return resp;
}

NameNodeResponse NameNodeServer::HandleAllocateBlock(const AllocateBlockRequest& req) {
    NameNodeResponse resp;
    if (!metadata_->GetFile(req.file_path()).has_value()) {
        resp.set_success(false);
        resp.set_error("File not found: " + req.file_path());
        return resp;
    }

    auto alive_nodes = dn_manager_->GetAliveDataNodes();
    if (alive_nodes.empty()) {
        resp.set_success(false);
        resp.set_error("No available DataNode");
        return resp;
    }

    try {
        BlockInfo block = block_manager_->AllocateBlock(alive_nodes);
        block_manager_->CommitBlock(block);

        auto f = metadata_->GetFile(req.file_path());
        if (f.has_value()) {
            f->blocks.push_back(block.block_id);
            metadata_->UpdateFile(req.file_path(), *f);
        }

        auto* ar = resp.mutable_allocate_block();
        ar->set_success(true);
        ar->set_block_id(block.block_id);
        for (const auto& dn : block.locations)
            ar->add_datanode_addresses(dn);
        resp.set_success(true);
    } catch (const std::exception& e) {
        resp.set_success(false);
        resp.set_error(e.what());
    }
    return resp;
}

NameNodeResponse NameNodeServer::HandleGetFileBlocks(const GetFileBlocksRequest& req) {
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
            for (const auto& dn : block->locations) bl->add_datanodes(dn);
        }
    }
    return resp;
}

NameNodeResponse NameNodeServer::HandleDeleteFile(const DeleteFileRequest& req) {
    NameNodeResponse resp;
    auto f = metadata_->GetFile(req.path());
    if (!f.has_value()) {
        resp.set_success(false);
        resp.set_error("File not found: " + req.path());
        return resp;
    }
    for (BlockID bid : f->blocks) metadata_->DeleteBlock(bid);
    metadata_->DeleteFile(req.path());
    resp.set_success(true);
    return resp;
}

NameNodeResponse NameNodeServer::HandleListFiles(const ListFilesRequest& req) {
    NameNodeResponse resp;
    resp.set_success(true);
    auto files = metadata_->ListFiles(req.dir());
    auto* lr   = resp.mutable_list_files();
    lr->set_success(true);
    for (const auto& f : files) {
        auto* fs = lr->add_files();
        fs->set_path(f.path);
        fs->set_size(f.size);
        fs->set_create_time(f.create_time);
        fs->set_block_count((int32_t)f.blocks.size());
    }
    return resp;
}

NameNodeResponse NameNodeServer::HandleRegisterDN(const RegisterDataNodeRequest& req) {
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

NameNodeResponse NameNodeServer::HandleHeartbeat(const HeartbeatRequest& req) {
    NameNodeResponse resp;
    bool ok = dn_manager_->HandleHeartbeat(
        req.datanode_id(), req.free_space(), req.block_count());
    resp.set_success(ok);
    if (!ok) resp.set_error("DataNode not registered: " + req.datanode_id());
    return resp;
}

NameNodeResponse NameNodeServer::HandleBlockReport(const BlockReportRequest& req) {
    NameNodeResponse resp;
    for (const auto& item : req.blocks()) {
        metadata_->UpdateBlockLocation(item.block_id(), req.datanode_id());
        // Also update block size if we see it for first time
        auto existing = metadata_->GetBlock(item.block_id());
        if (existing.has_value() && existing->size == 0 && item.size() > 0) {
            BlockInfo bi = *existing;
            bi.size = item.size();
            metadata_->UpdateBlock(bi);
        }
    }
    std::cout << "[NameNode] BlockReport from " << req.datanode_id()
              << ": " << req.blocks_size() << " blocks\n";
    resp.set_success(true);
    return resp;
}

void NameNodeServer::StartHealthCheckTimer() {
    health_check_thread_ = std::thread([this] {
        while (!stop_.load()) {
            std::this_thread::sleep_for(std::chrono::seconds(5));
            if (!stop_.load()) {
                dn_manager_->CheckDataNodeHealth();
                std::cout << "[NameNode] DataNodes: "
                          << dn_manager_->AliveCount() << " alive / "
                          << dn_manager_->TotalCount() << " total\n";
            }
        }
    });
}

}  // namespace mini_storage
