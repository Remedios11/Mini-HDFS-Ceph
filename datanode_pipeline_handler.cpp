#include "datanode_pipeline_handler.h"
#include "net_io_helpers.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <iostream>

namespace mini_storage {

PipelineHandler::PipelineHandler(BlockStore* block_store)
    : block_store_(block_store) {}

WriteBlockResponse PipelineHandler::HandleWrite(const WriteBlockRequest& req) {
    WriteBlockResponse resp;

    if (!block_store_->WriteBlock(req.block_id(), req.data())) {
        resp.set_success(false);
        resp.set_error_message("Failed to write block to disk");
        return resp;
    }
    std::cout << "[DataNode] Block " << req.block_id() << " written to disk\n";

    if (req.pipeline_size() > 0) {
        std::string next_node = req.pipeline(0);
        WriteBlockRequest forward_req;
        forward_req.set_block_id(req.block_id());
        forward_req.set_data(req.data());
        for (int i = 1; i < req.pipeline_size(); i++)
            forward_req.add_pipeline(req.pipeline(i));

        if (!ForwardToNext(next_node, forward_req)) {
            resp.set_success(false);
            resp.set_error_message("Failed to forward to " + next_node);
            return resp;
        }
        std::cout << "[DataNode] Forwarded block " << req.block_id()
                  << " to " << next_node << "\n";
    }

    resp.set_success(true);
    return resp;
}

bool PipelineHandler::ForwardToNext(const std::string& next_node,
                                     const WriteBlockRequest& req) {
    size_t colon = next_node.rfind(':');
    if (colon == std::string::npos) return false;
    std::string host = next_node.substr(0, colon);
    int port = std::stoi(next_node.substr(colon + 1));

    int fd = ConnectTo(host, port);
    if (fd < 0) return false;

    DataNodeRequest dn_req;
    dn_req.set_op(DataNodeRequest::WRITE_BLOCK);
    dn_req.set_request_id(req.block_id());
    *dn_req.mutable_write_block() = req;

    bool ok = SendProto(fd, dn_req);
    if (ok) {
        DataNodeResponse dn_resp;
        ok = RecvProto(fd, &dn_resp) && dn_resp.success();
    }
    close(fd);
    return ok;
}

int PipelineHandler::ConnectTo(const std::string& host, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    struct timeval tv { 5, 0 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    inet_pton(AF_INET, host.c_str(), &addr.sin_addr);

    if (connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

bool PipelineHandler::SendProto(int fd, const DataNodeRequest& req) {
    std::string bytes;
    req.SerializeToString(&bytes);
    return SendMsg(fd, bytes);
}

bool PipelineHandler::RecvProto(int fd, DataNodeResponse* resp) {
    std::string bytes;
    if (!RecvMsg(fd, &bytes)) return false;
    return resp->ParseFromString(bytes);
}

}  // namespace mini_storage
