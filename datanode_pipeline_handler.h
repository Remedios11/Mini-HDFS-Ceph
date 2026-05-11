#pragma once
#include "datanode_block_store.h"
#include "proto/datanode.pb.h"

namespace mini_storage {

class PipelineHandler {
public:
    explicit PipelineHandler(BlockStore* block_store);

    WriteBlockResponse HandleWrite(const WriteBlockRequest& req);

private:
    BlockStore* block_store_;

    bool ForwardToNext(const std::string& next_node, const WriteBlockRequest& req);
    int  ConnectTo(const std::string& host, int port);
    bool SendProto(int fd, const DataNodeRequest& req);
    bool RecvProto(int fd, DataNodeResponse* resp);
};

}  // namespace mini_storage
