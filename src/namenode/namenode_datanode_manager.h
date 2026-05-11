#pragma once
#include "common_types.h"
#include <unordered_map>
#include <vector>
#include <mutex>
#include <optional>
#include <chrono>

namespace mini_storage {

class DataNodeManager {
public:
    DataNodeManager() = default;

    bool RegisterDataNode(const DataNodeInfo& info);
    bool HandleHeartbeat(const std::string& id, int64_t free_space, int32_t block_count);

    std::vector<DataNodeInfo>   GetAliveDataNodes() const;
    std::optional<DataNodeInfo> GetDataNode(const std::string& id) const;

    void   CheckDataNodeHealth();
    size_t TotalCount() const;
    size_t AliveCount() const;

private:
    static int64_t NowMs() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    std::unordered_map<DataNodeID, DataNodeInfo> nodes_;
    mutable std::mutex                           mutex_;
};

}  // namespace mini_storage
