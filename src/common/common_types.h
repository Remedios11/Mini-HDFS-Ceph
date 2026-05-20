#pragma once
#include <string>
#include <vector>
#include <cstdint>

namespace mini_storage {

using BlockID    = uint64_t;
using FilePath   = std::string;
using DataNodeID = std::string;

constexpr BlockID kInvalidBlockID    = 0;
constexpr int64_t kBlockSize         = 4 * 1024 * 1024;  // 4MB
constexpr int     kReplicationFactor = 3;
constexpr int     kHeartbeatTimeoutSec  = 30;
constexpr int     kHeartbeatIntervalSec = 3;

struct BlockInfo {
    BlockID block_id = kInvalidBlockID;
    int64_t size     = 0;
    std::vector<DataNodeID> locations;
};

struct FileInfo {
    FilePath path;
    int64_t  size        = 0;
    int64_t  create_time = 0;
    int64_t  modify_time = 0;
    std::vector<BlockID> blocks;
};

enum class DataNodeStatus {
    ALIVE,
    SUSPECT,
    DEAD,
};

struct DataNodeInfo {
    DataNodeID id;
    std::string host;
    int         port           = 0;
    DataNodeStatus status      = DataNodeStatus::ALIVE;
    int64_t  last_heartbeat    = 0;
    int64_t  free_space        = 0;
    int32_t  block_count       = 0;
};

}  // namespace mini_storage
