#include "namenode_block_manager.h"
#include <stdexcept>
#include <algorithm>

namespace mini_storage {

BlockManager::BlockManager(MetadataStore* metadata)
    : metadata_(metadata) {}

BlockInfo BlockManager::AllocateBlock(const std::vector<DataNodeInfo>& available_nodes) {
    std::vector<DataNodeInfo> usable;
    for (const auto& dn : available_nodes) {
        if (dn.status == DataNodeStatus::ALIVE && dn.free_space >= kBlockSize) {
            usable.push_back(dn);
        }
    }
    if (usable.empty())
        throw std::runtime_error("No available DataNode to allocate block");

    BlockID bid      = next_block_id_.fetch_add(1);
    int     replicas = std::min((int)usable.size(), kReplicationFactor);
    auto    selected = SelectDataNodes(usable, replicas);

    BlockInfo block;
    block.block_id  = bid;
    block.size      = 0;
    block.locations = selected;
    return block;
}

void BlockManager::CommitBlock(const BlockInfo& block) {
    metadata_->AddBlock(block);
}

std::optional<BlockInfo> BlockManager::GetBlockInfo(BlockID block_id) const {
    return metadata_->GetBlock(block_id);
}

uint64_t BlockManager::GetBlockCount() const {
    return metadata_->BlockCount();
}

std::vector<DataNodeID> BlockManager::SelectDataNodes(
    const std::vector<DataNodeInfo>& available, int n) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<DataNodeID> selected;
    int sz    = (int)available.size();
    int start = round_robin_index_.load() % sz;
    for (int i = 0; i < n; i++) {
        int idx = (start + i) % sz;
        selected.push_back(available[idx].id);
    }
    round_robin_index_.store((start + n) % sz);
    return selected;
}

}  // namespace mini_storage
