#pragma once
// Week 8: 统一故障检测与自动恢复调度器
// 职责：定期运行，整合 ReplicationMonitor + BlockReplicator + ConsistencyChecker
#include "namenode_metadata_store.h"
#include "namenode_datanode_manager.h"
#include "namenode_replication_monitor.h"
#include "namenode_block_replicator.h"
#include "namenode_consistency_checker.h"
#include <thread>
#include <atomic>
#include <chrono>
#include <functional>

namespace mini_storage {

class FaultDetector {
public:
    // interval_sec: 多少秒运行一次完整检测
    FaultDetector(MetadataStore* metadata,
                  DataNodeManager* dn_manager,
                  int interval_sec = 10);
    ~FaultDetector();

    void Start();
    void Stop();

    // 立即执行一次完整的故障检测+修复（供测试直接调用）
    void RunOnce();

    // 注册回调：当检测到 DN 宕机时触发
    using DeadNodeCallback = std::function<void(const DataNodeID&)>;
    void SetDeadNodeCallback(DeadNodeCallback cb) { dead_node_cb_ = std::move(cb); }

    // 统计信息
    uint64_t TotalRepairRounds()   const { return repair_rounds_.load();   }
    uint64_t TotalBlocksRepaired() const { return blocks_repaired_.load(); }

private:
    void DetectLoop();
    void HandleDeadNodes();
    void RepairReplicas();
    void VerifyConsistency();

    MetadataStore*   metadata_;
    DataNodeManager* dn_manager_;
    int              interval_sec_;

    std::unique_ptr<ReplicationMonitor> rep_monitor_;
    std::unique_ptr<BlockReplicator>    replicator_;
    std::unique_ptr<ConsistencyChecker> checker_;

    std::thread       detect_thread_;
    std::atomic<bool> stop_{false};

    std::atomic<uint64_t> repair_rounds_{0};
    std::atomic<uint64_t> blocks_repaired_{0};

    DeadNodeCallback dead_node_cb_;
};

}  // namespace mini_storage
