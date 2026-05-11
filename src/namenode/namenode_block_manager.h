#pragma once
#include "common_types.h"
#include "namenode_metadata_store.h"
#include <atomic>
#include <mutex>

namespace mini_storage {

class BlockManager {
public:
    explicit BlockManager(MetadataStore* metadata);

    BlockInfo AllocateBlock(const std::vector<DataNodeInfo>& available_nodes);
    void CommitBlock(const BlockInfo& block);
    std::optional<BlockInfo> GetBlockInfo(BlockID block_id) const;
    uint64_t GetBlockCount() const;

private:
    std::vector<DataNodeID> SelectDataNodes(
        const std::vector<DataNodeInfo>& available, int n);

    MetadataStore*         metadata_;
    std::atomic<BlockID>   next_block_id_{1};
    std::atomic<int>       round_robin_index_{0};
    mutable std::mutex     mutex_;
};

}  // namespace mini_storage
