#include "namenode_replication_monitor.h"
#include <algorithm>
#include <iostream>
#include <set>

namespace mini_storage {

ReplicationMonitor::ReplicationMonitor(MetadataStore* metadata,
                                        DataNodeManager* dn_manager)
    : metadata_(metadata), dn_manager_(dn_manager) {}

std::vector<UnderReplicatedBlock> ReplicationMonitor::ScanUnderReplicated() {
    std::vector<UnderReplicatedBlock> result;

    // 获取所有存活 DN 的 ID 集合，用于快速查找
    auto alive_nodes = dn_manager_->GetAliveDataNodes();
    std::set<DataNodeID> alive_set;
    for (const auto& dn : alive_nodes) alive_set.insert(dn.id);

    // 遍历所有 block（通过 ListFiles 拿到所有文件的所有 block）
    auto files = metadata_->ListFiles("");
    std::set<BlockID> visited;

    for (const auto& file : files) {
        for (BlockID bid : file.blocks) {
            if (visited.count(bid)) continue;
            visited.insert(bid);

            auto block = metadata_->GetBlock(bid);
            if (!block.has_value()) continue;

            UnderReplicatedBlock urb;
            urb.block_id        = bid;
            urb.target_replicas = kReplicationFactor;

            // 分类：存活副本 vs 宕机副本
            for (const auto& dn_id : block->locations) {
                if (alive_set.count(dn_id)) {
                    urb.existing_locations.push_back(dn_id);
                } else {
                    urb.dead_locations.push_back(dn_id);
                }
            }
            urb.current_replicas = (int)urb.existing_locations.size();

            if (urb.current_replicas < kReplicationFactor) {
                result.push_back(urb);
                std::cout << "[ReplicationMonitor] Under-replicated block "
                          << bid << ": " << urb.current_replicas
                          << "/" << kReplicationFactor << " replicas\n";
            }
        }
    }
    return result;
}

UnderReplicatedBlock ReplicationMonitor::CheckBlock(BlockID block_id) {
    UnderReplicatedBlock urb;
    urb.block_id        = block_id;
    urb.target_replicas = kReplicationFactor;

    auto block = metadata_->GetBlock(block_id);
    if (!block.has_value()) return urb;

    auto alive_nodes = dn_manager_->GetAliveDataNodes();
    std::set<DataNodeID> alive_set;
    for (const auto& dn : alive_nodes) alive_set.insert(dn.id);

    for (const auto& dn_id : block->locations) {
        if (alive_set.count(dn_id)) {
            urb.existing_locations.push_back(dn_id);
        } else {
            urb.dead_locations.push_back(dn_id);
        }
    }
    urb.current_replicas = (int)urb.existing_locations.size();
    return urb;
}

ReplicationMonitor::ClusterHealth ReplicationMonitor::GetClusterHealth() {
    ClusterHealth health;

    auto alive_nodes = dn_manager_->GetAliveDataNodes();
    std::set<DataNodeID> alive_set;
    for (const auto& dn : alive_nodes) alive_set.insert(dn.id);

    health.alive_datanodes = (int)alive_nodes.size();
    health.dead_datanodes  = (int)(dn_manager_->TotalCount() - alive_nodes.size());

    auto files = metadata_->ListFiles("");
    std::set<BlockID> visited;

    for (const auto& file : files) {
        for (BlockID bid : file.blocks) {
            if (visited.count(bid)) continue;
            visited.insert(bid);
            health.total_blocks++;

            auto block = metadata_->GetBlock(bid);
            if (!block.has_value()) continue;

            int alive_count = 0;
            for (const auto& dn_id : block->locations) {
                if (alive_set.count(dn_id)) alive_count++;
            }

            if (alive_count == 0)
                health.lost_blocks++;
            else if (alive_count < kReplicationFactor)
                health.under_replicated++;
            else if (alive_count == kReplicationFactor)
                health.healthy_blocks++;
            else
                health.over_replicated++;
        }
    }
    return health;
}

}  // namespace mini_storage
