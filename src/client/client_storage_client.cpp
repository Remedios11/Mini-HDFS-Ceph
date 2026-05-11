#include "client_storage_client.h"
#include "net_io_helpers.h"
#include "proto/namenode.pb.h"
#include "proto/datanode.pb.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <iostream>
#include <cstring>

namespace mini_storage {

StorageClient::StorageClient(const std::string& nn_host, int nn_port)
    : nn_host_(nn_host), nn_port_(nn_port) {}

// ===== NameNode helpers =====

bool StorageClient::SendToNameNode(const std::string& req_bytes,
                                    std::string* resp_bytes) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;

    struct timeval tv { 10, 0 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(nn_port_);
    inet_pton(AF_INET, nn_host_.c_str(), &addr.sin_addr);

    if (connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd);
        return false;
    }

    if (!SendMsg(fd, req_bytes)) { close(fd); return false; }

    if (!RecvMsg(fd, resp_bytes)) { close(fd); return false; }
    close(fd);
    return true;
}

// ===== DataNode helpers =====

bool StorageClient::WriteBlockToDN(const std::string& dn_addr,
                                    uint64_t block_id,
                                    const std::string& data,
                                    const std::vector<std::string>& pipeline) {
    size_t colon = dn_addr.rfind(':');
    if (colon == std::string::npos) return false;
    std::string host = dn_addr.substr(0, colon);
    int port = std::stoi(dn_addr.substr(colon + 1));

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;

    struct timeval tv { 30, 0 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    inet_pton(AF_INET, host.c_str(), &addr.sin_addr);

    if (connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd);
        return false;
    }

    WriteBlockRequest wb;
    wb.set_block_id(block_id);
    wb.set_data(data);
    for (const auto& p : pipeline) wb.add_pipeline(p);

    DataNodeRequest req;
    req.set_op(DataNodeRequest::WRITE_BLOCK);
    req.set_request_id(block_id);
    *req.mutable_write_block() = wb;

    std::string bytes;
    req.SerializeToString(&bytes);
    if (!SendMsg(fd, bytes)) { close(fd); return false; }

    std::string resp_bytes;
    if (!RecvMsg(fd, &resp_bytes)) { close(fd); return false; }
    close(fd);

    DataNodeResponse resp;
    resp.ParseFromString(resp_bytes);
    return resp.success();
}

bool StorageClient::ReadBlockFromDN(const std::string& dn_addr,
                                     uint64_t block_id,
                                     std::string* data_out) {
    size_t colon = dn_addr.rfind(':');
    if (colon == std::string::npos) return false;
    std::string host = dn_addr.substr(0, colon);
    int port = std::stoi(dn_addr.substr(colon + 1));

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;

    struct timeval tv { 30, 0 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    inet_pton(AF_INET, host.c_str(), &addr.sin_addr);

    if (connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd);
        return false;
    }

    ReadBlockRequest rb;
    rb.set_block_id(block_id);
    rb.set_offset(0);
    rb.set_length(0);  // whole block

    DataNodeRequest req;
    req.set_op(DataNodeRequest::READ_BLOCK);
    req.set_request_id(block_id);
    *req.mutable_read_block() = rb;

    std::string bytes;
    req.SerializeToString(&bytes);
    if (!SendMsg(fd, bytes)) { close(fd); return false; }

    std::string resp_bytes;
    if (!RecvMsg(fd, &resp_bytes)) { close(fd); return false; }
    close(fd);

    DataNodeResponse resp;
    resp.ParseFromString(resp_bytes);
    if (!resp.success()) return false;
    *data_out = resp.read_block().data();
    return true;
}

// ===== High-level file operations =====

bool StorageClient::PutFile(const std::string& path, const std::string& data) {
    // 1. Create file on NameNode
    NameNodeRequest create_req;
    create_req.set_op(NameNodeRequest::CREATE_FILE);
    create_req.set_request_id(1);
    create_req.mutable_create_file()->set_path(path);

    std::string create_bytes, create_resp_bytes;
    create_req.SerializeToString(&create_bytes);
    if (!SendToNameNode(create_bytes, &create_resp_bytes)) return false;

    NameNodeResponse create_resp;
    create_resp.ParseFromString(create_resp_bytes);
    if (!create_resp.success()) {
        std::cerr << "[Client] CreateFile failed: " << create_resp.error() << "\n";
        return false;
    }

    // 2. Split data into blocks and write each block
    size_t offset   = 0;
    int    block_seq = 0;
    while (offset < data.size() || block_seq == 0) {
        size_t chunk_size = std::min((size_t)kBlockSize, data.size() - offset);
        std::string chunk = data.substr(offset, chunk_size);

        // Allocate block from NameNode
        NameNodeRequest alloc_req;
        alloc_req.set_op(NameNodeRequest::ALLOCATE_BLOCK);
        alloc_req.set_request_id(100 + block_seq);
        alloc_req.mutable_allocate_block()->set_file_path(path);

        std::string alloc_bytes, alloc_resp_bytes;
        alloc_req.SerializeToString(&alloc_bytes);
        if (!SendToNameNode(alloc_bytes, &alloc_resp_bytes)) return false;

        NameNodeResponse alloc_resp;
        alloc_resp.ParseFromString(alloc_resp_bytes);
        if (!alloc_resp.success() || !alloc_resp.has_allocate_block()) {
            std::cerr << "[Client] AllocateBlock failed: " << alloc_resp.error() << "\n";
            return false;
        }

        const auto& ab = alloc_resp.allocate_block();
        if (ab.datanode_addresses_size() == 0) {
            std::cerr << "[Client] No DataNodes returned\n";
            return false;
        }

        // Build pipeline: first DN gets data, rest are pipeline targets
        std::string primary = ab.datanode_addresses(0);
        std::vector<std::string> pipeline;
        for (int i = 1; i < ab.datanode_addresses_size(); i++)
            pipeline.push_back(ab.datanode_addresses(i));

        if (!WriteBlockToDN(primary, ab.block_id(), chunk, pipeline)) {
            std::cerr << "[Client] WriteBlockToDN failed for block "
                      << ab.block_id() << "\n";
            return false;
        }

        offset += chunk_size;
        block_seq++;
        if (offset >= data.size()) break;
    }
    return true;
}

bool StorageClient::GetFile(const std::string& path, std::string* data_out) {
    // 1. Get file block list from NameNode
    NameNodeRequest req;
    req.set_op(NameNodeRequest::GET_FILE_BLOCKS);
    req.set_request_id(200);
    req.mutable_get_file_blocks()->set_path(path);

    std::string req_bytes, resp_bytes;
    req.SerializeToString(&req_bytes);
    if (!SendToNameNode(req_bytes, &resp_bytes)) return false;

    NameNodeResponse resp;
    resp.ParseFromString(resp_bytes);
    if (!resp.success() || !resp.has_get_file_blocks()) {
        std::cerr << "[Client] GetFileBlocks failed: " << resp.error() << "\n";
        return false;
    }

    const auto& gfb = resp.get_file_blocks();
    data_out->clear();

    // 2. Read each block from any available DataNode
    for (int i = 0; i < gfb.blocks_size(); i++) {
        const auto& bl = gfb.blocks(i);
        if (bl.datanodes_size() == 0) {
            std::cerr << "[Client] Block " << bl.block_id() << " has no DataNodes\n";
            return false;
        }

        std::string block_data;
        bool ok = false;
        // Try each replica
        for (int j = 0; j < bl.datanodes_size() && !ok; j++) {
            ok = ReadBlockFromDN(bl.datanodes(j), bl.block_id(), &block_data);
        }
        if (!ok) {
            std::cerr << "[Client] Failed to read block " << bl.block_id() << "\n";
            return false;
        }
        data_out->append(block_data);
    }
    return true;
}

bool StorageClient::DeleteFile(const std::string& path) {
    NameNodeRequest req;
    req.set_op(NameNodeRequest::DELETE_FILE);
    req.set_request_id(300);
    req.mutable_delete_file()->set_path(path);

    std::string req_bytes, resp_bytes;
    req.SerializeToString(&req_bytes);
    if (!SendToNameNode(req_bytes, &resp_bytes)) return false;

    NameNodeResponse resp;
    resp.ParseFromString(resp_bytes);
    return resp.success();
}

}  // namespace mini_storage
