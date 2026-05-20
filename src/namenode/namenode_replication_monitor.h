#pragma once
// Week 8: 副本监控器
// 职责：扫描所有 block，找出副本数不足的，加入待修复队列
#include "common_types.h"
#include "namenode_metadata_store.h"
#include "namenode_datanode_manager.h"
#include <queue>
#include <mutex>
#include <vector>

namespace mini_storage {

struct UnderReplicatedBlock {
    BlockID  block_id;
    int      current_replicas;   // 当前存活副本数
    int      target_replicas;    // 目标副本数
    std::vector<DataNodeID> existing_locations;  // 已有副本在哪些 DN
    std::vector<DataNodeID> dead_locations;      // 已宕机的 DN
};

class ReplicationMonitor {
public:
    ReplicationMonitor(MetadataStore* metadata, DataNodeManager* dn_manager);

    // 扫描全部 block，返回所有副本不足的 block
    std::vector<UnderReplicatedBlock> ScanUnderReplicated();

    // 查询单个 block 的副本状态
    UnderReplicatedBlock CheckBlock(BlockID block_id);

    // 统计当前集群副本健康状态
    struct ClusterHealth {
        int total_blocks         = 0;
        int healthy_blocks       = 0;   // 副本数 == kReplicationFactor
        int under_replicated     = 0;   // 副本数 < kReplicationFactor 但 > 0
        int lost_blocks          = 0;   // 副本数 == 0（数据丢失）
        int over_replicated      = 0;   // 副本数 > kReplicationFactor
        int alive_datanodes      = 0;
        int dead_datanodes       = 0;
    };
    ClusterHealth GetClusterHealth();

private:
    MetadataStore*   metadata_;
    DataNodeManager* dn_manager_;
};

}  // namespace mini_storage
