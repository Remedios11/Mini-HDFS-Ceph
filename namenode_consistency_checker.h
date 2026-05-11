#pragma once
// Week 8: 数据一致性校验器
// 职责：向 DataNode 发送 block 读取请求，验证 CRC32，发现损坏则触发修复
#include "common_types.h"
#include "namenode_metadata_store.h"
#include "namenode_datanode_manager.h"
#include <string>
#include <vector>

namespace mini_storage {

struct BlockCheckResult {
    BlockID     block_id;
    DataNodeID  datanode_id;
    bool        reachable   = false;   // DN 是否可达
    bool        crc_ok      = false;   // CRC 是否正确
    uint32_t    actual_crc  = 0;
    int64_t     actual_size = 0;
    std::string error;
};

class ConsistencyChecker {
public:
    ConsistencyChecker(MetadataStore* metadata, DataNodeManager* dn_manager);

    // 检查单个 block 在某个 DN 上的完整性
    BlockCheckResult CheckBlockOnDN(BlockID block_id, const DataNodeID& dn_id);

    // 检查单个 block 的所有副本
    std::vector<BlockCheckResult> CheckBlockAllReplicas(BlockID block_id);

    // 全量扫描：检查所有 block 的所有副本
    // 返回有问题的检查结果列表
    std::vector<BlockCheckResult> ScanAllBlocks();

    // 打印集群一致性报告
    void PrintReport(const std::vector<BlockCheckResult>& results);

private:
    bool FetchAndVerify(const DataNodeID& dn_id, BlockID block_id,
                        std::string* data_out, uint32_t* crc_out);

    static uint32_t ComputeCRC32(const std::string& data);

    MetadataStore*   metadata_;
    DataNodeManager* dn_manager_;
};

}  // namespace mini_storage
