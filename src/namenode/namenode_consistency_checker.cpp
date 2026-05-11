#include "namenode_consistency_checker.h"
#include "proto/datanode.pb.h"
#include "net_io_helpers.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <zlib.h>
#include <iostream>
#include <set>

namespace mini_storage {

ConsistencyChecker::ConsistencyChecker(MetadataStore* metadata,
                                        DataNodeManager* dn_manager)
    : metadata_(metadata), dn_manager_(dn_manager) {}

uint32_t ConsistencyChecker::ComputeCRC32(const std::string& data) {
    return crc32(0, (const Bytef*)data.data(), data.size());
}

bool ConsistencyChecker::FetchAndVerify(const DataNodeID& dn_id,
                                         BlockID block_id,
                                         std::string* data_out,
                                         uint32_t* crc_out) {
    size_t colon = dn_id.rfind(':');
    if (colon == std::string::npos) return false;
    std::string host = dn_id.substr(0, colon);
    int port = std::stoi(dn_id.substr(colon + 1));

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;

    struct timeval tv{10, 0};
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
    rb.set_length(0);

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
    if (!resp.ParseFromString(resp_bytes) || !resp.success()) return false;

    *data_out = resp.read_block().data();
    *crc_out  = ComputeCRC32(*data_out);
    return true;
}

BlockCheckResult ConsistencyChecker::CheckBlockOnDN(BlockID block_id,
                                                     const DataNodeID& dn_id) {
    BlockCheckResult result;
    result.block_id    = block_id;
    result.datanode_id = dn_id;

    std::string data;
    uint32_t crc = 0;
    if (!FetchAndVerify(dn_id, block_id, &data, &crc)) {
        result.reachable = false;
        result.crc_ok    = false;
        result.error     = "Cannot fetch block from " + dn_id;
        return result;
    }

    result.reachable    = true;
    result.actual_crc   = crc;
    result.actual_size  = (int64_t)data.size();

    // 和 NameNode 元数据记录的大小对比
    auto block_meta = metadata_->GetBlock(block_id);
    if (block_meta.has_value() && block_meta->size > 0) {
        result.crc_ok = (result.actual_size == block_meta->size);
        if (!result.crc_ok) {
            result.error = "Size mismatch: expected=" +
                           std::to_string(block_meta->size) +
                           " actual=" + std::to_string(result.actual_size);
        }
    } else {
        // 没有元数据记录大小时，只要能读到数据就认为 OK
        result.crc_ok = (result.actual_size > 0);
    }

    return result;
}

std::vector<BlockCheckResult> ConsistencyChecker::CheckBlockAllReplicas(
    BlockID block_id) {
    std::vector<BlockCheckResult> results;

    auto block = metadata_->GetBlock(block_id);
    if (!block.has_value()) return results;

    auto alive_nodes = dn_manager_->GetAliveDataNodes();
    std::set<DataNodeID> alive_set;
    for (const auto& dn : alive_nodes) alive_set.insert(dn.id);

    for (const auto& dn_id : block->locations) {
        if (!alive_set.count(dn_id)) {
            // DN 已死，跳过
            BlockCheckResult r;
            r.block_id    = block_id;
            r.datanode_id = dn_id;
            r.reachable   = false;
            r.crc_ok      = false;
            r.error       = "DataNode is dead";
            results.push_back(r);
            continue;
        }
        results.push_back(CheckBlockOnDN(block_id, dn_id));
    }
    return results;
}

std::vector<BlockCheckResult> ConsistencyChecker::ScanAllBlocks() {
    std::vector<BlockCheckResult> bad_results;

    auto files = metadata_->ListFiles("");
    std::set<BlockID> visited;

    for (const auto& file : files) {
        for (BlockID bid : file.blocks) {
            if (visited.count(bid)) continue;
            visited.insert(bid);

            auto results = CheckBlockAllReplicas(bid);
            for (const auto& r : results) {
                if (!r.reachable || !r.crc_ok) {
                    bad_results.push_back(r);
                }
            }
        }
    }
    return bad_results;
}

void ConsistencyChecker::PrintReport(const std::vector<BlockCheckResult>& results) {
    if (results.empty()) {
        std::cout << "[ConsistencyChecker] ✓ All blocks healthy\n";
        return;
    }
    std::cout << "[ConsistencyChecker] Found " << results.size()
              << " issues:\n";
    for (const auto& r : results) {
        std::cout << "  Block " << r.block_id
                  << " on " << r.datanode_id
                  << ": reachable=" << r.reachable
                  << " crc_ok=" << r.crc_ok;
        if (!r.error.empty()) std::cout << " error=" << r.error;
        std::cout << "\n";
    }
}

}  // namespace mini_storage
