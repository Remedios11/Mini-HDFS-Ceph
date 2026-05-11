#pragma once
#include <string>
#include <vector>
#include "common_types.h"

namespace mini_storage {

class StorageClient {
public:
    StorageClient(const std::string& namenode_host, int namenode_port);

    bool PutFile(const std::string& path, const std::string& data);
    bool GetFile(const std::string& path, std::string* data_out);
    bool DeleteFile(const std::string& path);

private:
    std::string nn_host_;
    int         nn_port_;

    bool SendToNameNode(const std::string& req_bytes, std::string* resp_bytes);
    bool WriteBlockToDN(const std::string& dn_addr, uint64_t block_id,
                        const std::string& data,
                        const std::vector<std::string>& pipeline);
    bool ReadBlockFromDN(const std::string& dn_addr, uint64_t block_id,
                         std::string* data_out);
};

}  // namespace mini_storage
