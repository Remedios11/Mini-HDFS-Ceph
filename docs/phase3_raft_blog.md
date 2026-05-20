# Mini-HDFS-Ceph 第三阶段：基于 Raft 实现 NameNode 高可用

## 前言

在前两个阶段中，我们构建了一个受 HDFS、Ceph 和 LevelDB 启发的分布式存储系统：

- **第一阶段**：实现了单机 LSM-tree 存储引擎（MemTable、WAL、SSTable、Compaction）
- **第二阶段**：扩展为分布式架构，引入 NameNode（元数据管理）和 DataNode（数据存储）的角色分离，支持文件分块、三副本写入、心跳检测和故障自动修复

但第二阶段存在一个致命缺陷：**NameNode 是单点**。一旦 NameNode 宕机，整个集群的元数据服务就不可用——无法创建文件、无法分配 Block、无法查询文件位置。这正是 HDFS 在 2.x 版本之前长期被诟病的问题。

第三阶段的目标很明确：**通过 Raft 共识协议实现 NameNode 的高可用**。

---

## 整体架构

```
┌─────────────────────────────────────────────────┐
│                  Client                          │
│            (StorageClient)                       │
└─────────────┬───────────────────────────────────┘
              │
              ▼
   ┌──────────────────────┐
   │   HA NameNode 集群    │
   │                      │
   │  ┌──────┐ ┌──────┐  │    Raft 共识
   │  │ NN-0 │◄┤ NN-1 │  │   ◄──────────
   │  │Leader│ │Folwr │  │
   │  └──┬───┘ └──────┘  │
   │     │                │
   │  ┌──────┐            │
   │  │ NN-2 │            │
   │  │Folwr │            │
   │  └──────┘            │
   └──────────┬───────────┘
              │
    ┌─────────┼─────────┐
    ▼         ▼         ▼
┌──────┐ ┌──────┐ ┌──────┐
│ DN-0 │ │ DN-1 │ │ DN-2 │   DataNode 集群
└──────┘ └──────┘ └──────┘
```

核心设计原则：

- **写操作走 Raft**（CreateFile、DeleteFile、AllocateBlock），通过领导者提议 → 日志复制 → 多数派提交 → 状态机应用的完整路径，保证所有 NameNode 的元数据最终一致
- **读操作本地服务**（GetFileBlocks、ListFiles），无需经过 Raft，直接从本地 MetadataStore 返回，性能无损
- **运维操作不参与共识**（DataNode 注册、心跳、Block Report），每个 NameNode 独立维护自己对 DataNode 集群的视图

---

## Raft 核心实现

### 1. 协议概览

Raft 将共识问题分解为三个相对独立的子问题：

| 子问题 | RPC | 我们的实现 |
|--------|-----|-----------|
| 领导者选举 | RequestVote | 随机超时 + 多数派投票 + 日志完备性检查 |
| 日志复制 | AppendEntries | 领导者驱动 + 快速冲突回溯 + 多数派提交 |
| 快照压缩 | InstallSnapshot | 定时快照 + 增量安装 + 日志截断 |

整个协议用 Protocol Buffers 定义（`proto/raft.proto`），包括 6 种消息类型。核心 Raft 节点实现在 `src/raft/raft_node.cpp`，约 775 行 C++ 代码，完整覆盖了 Raft 论文中描述的所有机制。

### 2. 数据结构

```cpp
// Raft 节点的核心状态
uint64_t current_term_;      // 当前任期号（持久化）
string   voted_for_;         // 本轮投票给了谁（持久化）
vector<LogEntry> log_;       // 日志条目 [1..n]，log_[0] 是哨兵（持久化）
uint64_t commit_index_;      // 已提交的最高日志索引
uint64_t last_applied_;      // 已应用到状态机的最高索引

// 领导者特有的易失状态
unordered_map<string, uint64_t> next_index_;   // 发给每个对等节点的下一条日志索引
unordered_map<string, uint64_t> match_index_;  // 每个对等节点已复制的最高日志索引
```

每个 LogEntry 包含 `{term, index, command}`，其中 command 是序列化后的 NameNode 操作请求（protobuf bytes）。

### 3. 领导者选举

选举的核心是**随机超时**机制，这是 Raft 避免选票分裂的关键：

```cpp
int RaftNode::RandomElectionTimeout() const {
    static thread_local std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<int> dist(
        config_.election_timeout_ms,          // 默认 2000ms
        config_.election_timeout_ms * 2);     // 默认 4000ms
    return dist(gen);
}
```

选举流程：

1. **Follower** 在随机超时（2~4 秒）内未收到 Leader 的心跳，转为 **Candidate**
2. Candidate 自增 `current_term`，给自己投票，向所有 peer 并行发送 `RequestVote`
3. 投票规则：
   - 请求的 `term` 必须 ≥ 自己的 `term`
   - 自己尚未在本 term 投过票（或已投给该候选人）
   - 候选人的日志比自己"新"（`last_log_term` 更大，或 term 相同但 `last_log_index` 更大）
4. 收集到**多数派**（`peers.size() / 2 + 1`）投票后，转为 **Leader**
5. 单节点集群直接成为 Leader（无需网络请求）

```cpp
// 多数派计算
int majority = (int)config_.peers.size() / 2 + 1;
if (votes_received_ >= majority) {
    BecomeLeader();
}
```

### 4. 日志复制

日志复制是 Raft 的核心。Leader 通过 `AppendEntries` RPC 将日志条目复制到所有 Follower：

```
Client ──Propose──► Leader ──AppendEntries──► Follower(s)
                      │                           │
                      │◄─── majority ACK ────────┘
                      │
                    commit_index++
                      │
                    apply_cb_()  ──► MetadataStore 更新
```

**快速冲突回溯**是我们对标准 Raft 的一个重要优化。标准的冲突处理是每次将 `next_index` 减 1 重试，在日志差异较大时需要 O(N) 次往返。我们在 `AppendEntriesResponse` 中增加了 `match_index` 字段，让 Follower 在拒绝时直接告诉 Leader 自己最大的匹配位置：

```cpp
// Follower 端：发现冲突时返回自己的最后匹配索引
if (prev_log_index > log_.back().index) {
    ae_resp_.set_success(false);
    ae_resp_.set_match_index(log_.back().index);  // 快速回溯
    return &ae_resp_;
}

// Leader 端：直接跳转到 Follower 的报告位置
if (!resp.success()) {
    uint64_t match = resp.match_index();
    next_index_[from] = std::max(match + 1, snapshot_last_index_ + 1);
}
```

这样一次往返就能完成冲突定位，无论日志差异有多大。

**提交**采用多数派原则：Leader 维护每个 peer 的 `match_index`，当某个索引 N 的条目已被多数节点复制后（且该条目的 term 等于 `current_term`，即 Leader 只能提交自己任期内的条目），推进 `commit_index`：

```cpp
void RaftNode::AdvanceCommitIndex() {
    uint64_t last = log_.back().index;
    for (uint64_t n = last; n > commit_index_; n--) {
        const LogEntry* entry = GetLogEntry(n);
        if (!entry || entry->term != current_term_) continue;
        int count = 1;  // self
        for (const auto& peer : config_.peers) {
            if (peer == my_id_) continue;
            if (match_index_[peer] >= n) count++;
        }
        if (count >= (int)config_.peers.size() / 2 + 1) {
            commit_index_ = n;
            ApplyCommitted(commit_index_);
            break;
        }
    }
}
```

### 5. 客户端 Propose

客户端（或 HA NameNode）通过 `Propose()` 方法提交写操作，这是一个同步阻塞接口：

```cpp
ProposeResult RaftNode::Propose(const string& command, int timeout_ms) {
    // 1. 检查是否为 Leader，不是则返回 leader_id 供客户端重定向
    if (state_ != RaftState::LEADER) {
        result.leader_id = leader_id_;
        return result;
    }

    // 2. 追加到本地日志
    LogEntry entry{current_term_, log_.back().index + 1, command};
    log_.push_back(entry);
    PersistState();

    // 3. 立即广播心跳（携带新条目）
    SendHeartbeats();

    // 4. 阻塞等待提交或超时（通过 condition_variable）
    while (commit_index_ < result.index && state_ == RaftState::LEADER) {
        propose_cv_.wait_until(lock, deadline);
    }
    return result;
}
```

如果当前节点不是 Leader，返回的 `leader_id` 可以让客户端**重定向**到正确的 Leader，这是实现高可用的关键——客户端不需要预先知道谁是 Leader。

### 6. 持久化与恢复

Raft 的安全性依赖持久化状态的正确性。我们将 `current_term`、`voted_for` 和完整日志序列化到二进制文件 `raft_state.dat`：

```
┌──────────────────────────────────────────────┐
│  current_term  (8 bytes)                     │
│  voted_for_len  (8 bytes)                    │
│  voted_for      (variable)                   │
│  log_count      (8 bytes)                    │
│  ┌─ entry[1]: term(8) + index(8) +          │
│  │            cmd_len(8) + cmd(variable)     │
│  ├─ entry[2]: ...                            │
│  └─ entry[N]: ...                            │
└──────────────────────────────────────────────┘
```

重启时，`RestoreState()` 读取该文件并恢复全部状态。`RestoreFromSnapshot()` 进一步用快照数据恢复状态机，然后截断已压缩的日志部分。

### 7. 快照与日志压缩

随着系统运行，Raft 日志会不断增长。如果不压缩，重启时的恢复时间和磁盘占用都将不可接受。我们实现了定时快照机制：

**快照创建**（Leader 侧，每 10 秒检查，日志超过 50 条时触发）：

```cpp
// RaftNode::TakeSnapshot()
string data = snapshot_cb_();  // 回调：序列化 MetadataStore 的完整状态
// 保存为 raft_snapshot.dat
// 截断 log_：删除所有 index <= last_included_index 的条目
```

**快照安装**（Follower 侧）：

```cpp
InstallSnapshotResponse* HandleInstallSnapshot(const InstallSnapshotRequest& req) {
    // 1. 保存快照文件（原子写入：先写 .tmp 再 rename）
    // 2. 调用 restore_cb_ 恢复状态机
    // 3. 更新 snapshot_last_index_ / snapshot_last_term_
    // 4. 截断日志
}
```

快照的元数据内容（`MetadataStoreSnapshot`）包含所有文件和 Block 的完整信息，足以让一个新节点从零恢复。

---

## 网络层：RaftRPC

Raft 节点之间的通信通过 `RaftRPC` 模块实现，它复用了项目第二阶段的自定义事件循环和 TCP 服务器组件。

架构设计：

```
RaftRPC
├── TcpServer（事件循环驱动，监听 raft_port）
│   └── OnMessage()：接收消息 → 解析 → 路由到 RaftNode → 发送响应
└── SendMessage()：主动连接 peer → 发送 → 读取响应 → 回调 RaftNode
```

**接收侧**运行在事件循环中，非阻塞。消息到达后解析为 `RaftMessage`，根据类型分发：

- `RequestVote` / `AppendEntries` / `InstallSnapshot`：调用 RaftNode 的 handler，立即通过同一 TCP 连接回复
- `RequestVoteResponse` / `AppendEntriesResponse`：异步交付给 RaftNode 处理

**发送侧**采用**短连接**模式：connect → send → recv response → close。每次心跳/请求都建立新连接。这个设计足够简单，对于 Raft 的通信模式（每 500ms 一次心跳，每个 peer 一条消息）来说性能完全够用。如果未来需要优化延迟，可以改为长连接复用。

每条消息设置了 3 秒的 socket 超时，防止网络故障导致线程阻塞。

---

## HA NameNode：状态机集成

`HANameNodeServer`（`src/namenode/namenode_ha_server.cpp`）是连接 Raft 共识层和 NameNode 业务逻辑的桥梁。它通过四个回调函数与 RaftNode 交互：

```
raft_node_ ──ApplyCallback────► OnApplyCommitted()  ──► MetadataStore 更新
raft_node_ ──SendRPCCallback──► OnSendRaftRPC()      ──► raft_rpc_->SendMessage()
raft_node_ ──SnapshotCallback─► TakeMetadataSnapshot()──► 序列化 MetadataStore
raft_node_ ──RestoreCallback──► RestoreMetadataSnapshot()► 反序列化恢复
```

**请求分流**的核心逻辑在 `ProcessClientRequest()`：

```cpp
switch (req.op()) {
case CREATE_FILE:
case DELETE_FILE:
case ALLOCATE_BLOCK:
    // 写操作走 Raft → ProposeWrite()
    resp = ProposeWrite(req);
    break;
case GET_FILE_BLOCKS:
case LIST_FILES:
    // 读操作本地服务
    resp = HandleGetFileBlocks(req.get_file_blocks());
    break;
case REGISTER_DN:
case HEARTBEAT:
case BLOCK_REPORT:
    // 运维操作直接处理，不参与共识
    resp = HandleRegisterDN(req.register_dn());
    break;
}
```

**写操作的完整路径**（以 CreateFile 为例）：

```
Client ──► HANameNodeServer
              │
              ├─ 检查：我是 Leader 吗？
              │    ├─ 否 → 返回 leader_id，客户端重定向
              │    └─ 是 → 继续
              │
              ├─ 序列化 NameNodeRequest → command bytes
              ├─ raft_node_->Propose(command, timeout_ms)
              │    ├─ Leader 追加到本地日志
              │    ├─ 广播 AppendEntries 到所有 Follower
              │    ├─ 等待多数派确认
              │    └─ commit_index 推进 → apply_cb_ 调用
              │
              ├─ OnApplyCommitted() 反序列化 → ApplyCreateFile()
              └─ 返回结果给客户端
```

所有节点（包括 Follower）都会执行 `OnApplyCommitted()`，保证状态机的一致性。

---

## 测试策略

我们针对 Raft 和 HA NameNode 编写了两套完整的测试：

### 单元与集成测试（10 个）

| 测试 | 验证内容 |
|------|---------|
| RaftNode 持久化 | 写入 5 条 → 重启 → 日志完整恢复 |
| 单节点选举 | 独立节点在超时后自动成为 Leader |
| 投票逻辑 | 任期比较、日志比较、重复投票拒绝 |
| AppendEntries 逻辑 | 正常复制、冲突检测、错误的 prev_log_index |
| 三节点选举 | 恰好产生一个 Leader |
| HA NameNode 集群基本操作 | 3 NN + 3 DN，PutFile/GetFile/DeleteFile |
| HA 故障切换 | Leader 崩溃 → 新 Leader 选举 → 继续服务 |
| 快照与恢复 | 创建快照 → 截断日志 → 从快照恢复 |
| RaftRPC 收发 | 基本消息收发 |
| 写走 Raft / 读本地 | Follower 可服务读请求 |

### 压力测试（8 个）

| 测试 | 场景 | 验证目标 |
|------|------|---------|
| 大规模日志复制 | 1000 条连续写入 | 日志增长的性能线性度 |
| 并发 Propose | 8 线程 × 50 条 | 并发提交的正确性 |
| Leader 频繁切换 | 5 轮 kill + 重选 | 切换期间的日志完整性 |
| 网络分区与恢复 | 模拟分区然后愈合 | 分区期间和恢复后的一致性 |
| 大负载 | 单条命令 256KB | 大 payload 处理能力 |
| 压力下的持久化 | 500 条 → 重启 → 验证 | 大规模状态恢复 |
| 快速选举抖动 | 100ms 超时 × 10 秒 | 极端条件下的稳定性 |
| HA 端到端压力 | 50 文件 PUT/GET + 延迟统计 | 真实场景性能 |

---

## 实战：启动一个三节点 HA NameNode 集群

```bash
# 节点 0（会成为初始 Leader）
./namenode_ha --client_port=9000 --raft_port=9001 \
  --peers=127.0.0.1:9001,127.0.0.1:9101,127.0.0.1:9201 \
  --data_dir=/data/nn0

# 节点 1
./namenode_ha --client_port=9100 --raft_port=9101 \
  --peers=127.0.0.1:9001,127.0.0.1:9101,127.0.0.1:9201 \
  --data_dir=/data/nn1

# 节点 2
./namenode_ha --client_port=9200 --raft_port=9201 \
  --peers=127.0.0.1:9001,127.0.0.1:9101,127.0.0.1:9201 \
  --data_dir=/data/nn2
```

启动后，三个节点通过 Raft 协议自动选出 Leader。客户端可以连接任意一个 NameNode 的 `client_port`：

- 如果连接到 Leader：写操作正常进行，读操作本地返回
- 如果连接到 Follower：写操作会收到 "Not leader. Leader: 127.0.0.1:9001" 的错误，客户端据此重定向到 Leader；读操作直接本地返回

当 Leader 宕机时，剩余节点在 2~4 秒内完成新 Leader 选举，集群恢复写入服务。

---

## 关键设计决策与权衡

### 1. 读操作不走 Raft

严格来说，在 Raft 的**线性一致性**语义下，读操作也应该经过 Leader 的日志复制来防止"脏读"（已提交但尚未应用到状态机的数据）。但我们选择了**直接本地读**：

- **代价**：Follower 可能返回稍旧的数据（最终一致性）
- **收益**：读性能完全不受 Raft 影响，且读请求可以分散到所有节点
- **为什么可以接受**：文件系统的元数据读操作（ListFiles、GetFileBlocks）对实时强一致性的要求不高，而心跳和 Block Report 已经提供了最终一致性的收敛路径

### 2. 短连接 RPC vs 长连接

当前每个 Raft 消息使用独立的 TCP 连接。对于 3~5 节点的 NameNode 集群和 500ms 的心跳间隔，开销可以忽略。但如果扩展到更大集群或更激进的心跳频率，可以改为长连接复用，减少 TCP 握手的开销。

### 3. 快照触发的时机

当前策略是"日志超过 50 条时触发快照"。在生产环境中应该基于日志字节数或日志条目的累积量来决策，避免小条目时的频繁快照或大条目时的日志膨胀。

---

## 总结

通过本阶段的实现，Mini-HDFS-Ceph 从"能用"迈向"可靠"。Raft 共识协议为 NameNode 提供了自动故障切换能力，整个系统不再有单点故障。

回顾三个阶段的演进：

| 阶段 | 核心能力 | 单点风险 |
|------|---------|---------|
| Phase 1 | 单机 LSM-tree 存储引擎 | 整个服务 |
| Phase 2 | 分布式 NameNode + DataNode 架构 | NameNode |
| Phase 3 | Raft 共识 → HA NameNode | 无（允许少数节点故障） |

完整的代码可以在项目仓库中找到——从头构建一个分布式存储系统是理解 HDFS、Ceph 和 Raft 的最佳方式。
