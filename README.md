# Mini-HDFS-Ceph

一个从零构建的分布式文件存储系统，融合了 **HDFS 架构**（NameNode/DataNode 分离）、**Ceph 思想**（多副本复制）和 **LevelDB 存储引擎**（LSM-Tree）。

[![C++17](https://img.shields.io/badge/C%2B%2B-17-blue)](https://en.cppreference.com/w/cpp/17)
[![Build](https://img.shields.io/badge/build-CMake-green)](https://cmake.org/)
[![Protocol Buffers](https://img.shields.io/badge/proto-proto3-orange)](https://protobuf.dev/)

---

## 目录

- [架构概览](#架构概览)
- [核心特性](#核心特性)
- [快速开始](#快速开始)
- [构建与测试](#构建与测试)
- [使用示例](#使用示例)
- [项目结构](#项目结构)
- [分阶段设计](#分阶段设计)
- [Raft 高可用](#raft-高可用)
- [性能与可靠性](#性能与可靠性)

---

## 架构概览

```
┌────────────────────────────────────────────────────────┐
│                    Client (StorageClient)               │
│            PutFile / GetFile / DeleteFile               │
└──────────────────────┬─────────────────────────────────┘
                       │
                       ▼
┌────────────────────────────────────────────────────────┐
│            HA NameNode Cluster (Raft Consensus)         │
│   ┌─────────┐      ┌─────────┐      ┌─────────┐       │
│   │ NN-0    │◄────►│ NN-1    │◄────►│ NN-2    │       │
│   │ Leader  │      │Follower │      │Follower │       │
│   └─────────┘      └─────────┘      └─────────┘       │
│   元数据管理 · 块分配 · 故障检测 · 复制监控              │
└──────────────────────┬─────────────────────────────────┘
                       │
                       ▼
┌────────────────────────────────────────────────────────┐
│                  DataNode Cluster (3-way Replication)   │
│   ┌─────────┐       ┌─────────┐      ┌─────────┐      │
│   │ DN-0    │       │ DN-1    │      │ DN-2    │      │
│   │ Block   │       │ Block   │      │ Block   │      │
│   │ Store   │       │ Store   │      │ Store   │      │
│   └─────────┘       └─────────┘      └─────────┘      │
│   块存储 · CRC校验 · 流水线复制 · 心跳上报              │
└────────────────────────────────────────────────────────┘
```

- **NameNode**：管理文件系统元数据（文件→块的映射、块→DataNode 的映射），负责块分配和故障检测
- **DataNode**：在本地磁盘上存储数据块，每个块附带 CRC32 校验，支持流水线式多副本写入
- **Client**：提供 `PutFile` / `GetFile` / `DeleteFile` 接口，自动处理文件分块和组装
- **Raft**：NameNode 通过 Raft 共识协议实现高可用，消除单点故障

---

## 核心特性

### 存储引擎（LSM-Tree）
- **MemTable**：基于跳表（Skip List）的内存表，支持 16 级索引
- **WAL**（Write-Ahead Log）：写前日志，保证崩溃恢复，每条记录带 CRC32 校验
- **SSTable**：磁盘上有序字符串表，含数据块、索引块、Bloom Filter 和 Footer
- **Bloom Filter**：快速判断键是否不存在，减少无效磁盘读取
- **LRU Cache**：基于哈希表 + 双向链表的数据块缓存
- **Compaction**：后台合并多个有序 SSTable，回收过期数据

### 分布式架构
- **元数据与数据分离**：NameNode 只管理元数据，DataNode 只存储数据
- **3 副本复制**：每个数据块通过流水线（Pipeline）方式写入 3 个 DataNode
- **心跳机制**：DataNode 每 5 秒向 NameNode 发送心跳，NameNode 超时 30 秒判定节点死亡
- **块报告**：DataNode 启动时向 NameNode 报告所有本地块，用于一致性校验
- **轮询分配**：NameNode 在健康 DataNode 中轮询选择块存放位置

### 高可用（Raft）
- **Leader 选举**：随机超时 2-4 秒，避免选票分裂
- **日志复制**：写操作通过 Raft `Propose()` 提交，多数派确认后返回
- **快照/压缩**：定期生成元数据快照，压缩 Raft 日志，控制日志体积
- **快速回溯**：AppendEntries 冲突时使用二分查找加速日志对齐
- **读本地**：读操作直接在本节点处理，无需 Raft 共识

### 故障检测与自动修复
- **ReplicationMonitor**：扫描所有块，发现副本数不足的块
- **BlockReplicator**：从健康 DataNode 复制块到新 DataNode，恢复副本数
- **ConsistencyChecker**：校验所有副本的 CRC32，检测数据损坏

---

## 快速开始

### 前置依赖

| 依赖 | 版本 | 说明 |
|------|------|------|
| C++ 编译器 | GCC 7+ / Clang 5+ | 支持 C++17 |
| CMake | ≥ 3.10 | 构建系统 |
| Protocol Buffers | ≥ 3.0 | RPC 序列化 |
| ZLIB | 任意 | 压缩支持 |
| Linux | 内核 3.0+ | epoll 异步 I/O |

### 安装依赖

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install -y build-essential cmake protobuf-compiler libprotobuf-dev zlib1g-dev
```

**CentOS/RHEL/Fedora:**
```bash
sudo dnf install -y gcc-c++ cmake protobuf-compiler protobuf-devel zlib-devel
```

### 构建

```bash
git clone https://github.com/Remedios11/Mini-HDFS-Ceph.git
cd Mini-HDFS-Ceph

# 配置 & 编译
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

### 运行测试

```bash
# 运行全部测试
ctest --output-on-failure

# 或单独运行
./test_phase1           # LSM 引擎测试
./test_end_to_end       # 端到端集成测试
./test_phase3_raft      # Raft 高可用测试
./test_raft_stress      # Raft 压力测试
```

---

## 使用示例

### 1. 启动存储引擎（单机 LSM-Tree）

```bash
# Phase 1: 单机 kv 存储（内嵌于测试代码中）
./test_phase1
```

### 2. 启动分布式集群

```bash
# 终端 1: 启动 NameNode
./namenode_server 9000 /tmp/nn_data

# 终端 2-4: 启动 3 个 DataNode
./datanode_server 9001 /tmp/dn0_data 127.0.0.1 9000
./datanode_server 9002 /tmp/dn1_data 127.0.0.1 9000
./datanode_server 9003 /tmp/dn2_data 127.0.0.1 9000
```

### 3. 启动 HA 集群（Raft 高可用）

```bash
# 3 节点 Raft NameNode 集群
./namenode_ha --client_port=8000 --raft_port=8001 --peers="127.0.0.1:8001,127.0.0.1:8101,127.0.0.1:8201" --data_dir="/tmp/nn0"
./namenode_ha --client_port=8100 --raft_port=8101 --peers="127.0.0.1:8001,127.0.0.1:8101,127.0.0.1:8201" --data_dir="/tmp/nn1"
./namenode_ha --client_port=8200 --raft_port=8201 --peers="127.0.0.1:8001,127.0.0.1:8101,127.0.0.1:8201" --data_dir="/tmp/nn2"

# 同时启动 DataNode（连接任意 NameNode）
./datanode_server 9001 /tmp/dn0 127.0.0.1 8000
./datanode_server 9002 /tmp/dn1 127.0.0.1 8000
./datanode_server 9003 /tmp/dn2 127.0.0.1 8000
```

### 4. 客户端操作（编程接口）

```cpp
#include "client/client_storage_client.h"

StorageClient client("127.0.0.1", 8000);

// 写入文件
std::string data = "Hello, Distributed Storage!";
client.PutFile("/hello.txt", data);

// 读取文件
auto result = client.GetFile("/hello.txt");
// result == "Hello, Distributed Storage!"

// 删除文件
client.DeleteFile("/hello.txt");
```

---

## 项目结构

```
Mini-HDFS-Ceph/
├── CMakeLists.txt                    # CMake 构建配置
├── proto/                            # Protobuf 协议定义
│   ├── namenode.proto                #   NameNode RPC 协议
│   ├── datanode.proto                #   DataNode RPC 协议
│   ├── raft.proto                    #   Raft 共识协议
│   └── storage.proto                 #   存储引擎协议
├── src/
│   ├── common/
│   │   └── common_types.h            #   公共类型定义
│   ├── engine/                       # Phase 1: LSM-Tree 存储引擎
│   │   ├── db.h / .cpp               #   LSM-Tree 数据库主类
│   │   ├── MemTable.h / .cpp         #   跳表内存表
│   │   ├── SSTableBuilder.h / .cpp   #   SSTable 构建器
│   │   ├── SSTableReader.h / .cpp    #   SSTable 读取器
│   │   ├── log_reader.h / .cpp       #   WAL 读取器
│   │   ├── log_writer.h / .cpp       #   WAL 写入器
│   │   ├── BloomFilter.h             #   布隆过滤器
│   │   ├── Lrucache.h / .cpp         #   LRU 缓存
│   │   ├── coding.h / .cpp           #   二进制编解码
│   │   ├── db_write_batch.h / .cpp   #   批量写入
│   │   └── log_format.h             #   WAL 格式常量
│   ├── namenode/                     # Phase 2-3: NameNode 服务
│   │   ├── namenode_namenode_server.h / .cpp  #   核心 RPC 服务
│   │   ├── namenode_metadata_store.h / .cpp   #   元数据存储
│   │   ├── namenode_block_manager.h / .cpp    #   块分配管理
│   │   ├── namenode_datanode_manager.h / .cpp #   DataNode 管理
│   │   ├── namenode_edit_log.h / .cpp         #   编辑日志
│   │   ├── namenode_replication_monitor.h/.cpp #   复制监控
│   │   ├── namenode_block_replicator.h / .cpp  #   块复制器
│   │   ├── namenode_consistency_checker.h/.cpp #   一致性检查
│   │   ├── namenode_fault_detector.h / .cpp    #   故障检测调度
│   │   └── namenode_ha_server.h / .cpp         #   HA 服务（集成 Raft）
│   ├── datanode/                     # Phase 2: DataNode 服务
│   │   ├── datanode_block_store.h / .cpp         #   块存储
│   │   ├── datanode_datanode_server.h / .cpp     #   RPC 服务
│   │   ├── datanode_datanode_client.h / .cpp     #   NameNode 客户端
│   │   └── datanode_pipeline_handler.h / .cpp    #   复制流水线
│   ├── client/
│   │   └── client_storage_client.h / .cpp  #   客户端库
│   ├── net/                          # 自研异步网络库
│   │   ├── net_event_loop.h / .cpp   #   epoll 事件循环
│   │   ├── net_tcp_server.h / .cpp   #   TCP 服务器
│   │   ├── net_tcp_connection.h/.cpp #   TCP 连接
│   │   ├── net_channel.h / .cpp      #   Channel 抽象
│   │   ├── net_thread_pool.h / .cpp  #   线程池
│   │   └── net_io_helpers.h         #   I/O 工具函数
│   ├── raft/                         # Phase 3: Raft 共识
│   │   ├── raft_node.h / .cpp        #   Raft 状态机核心
│   │   └── raft_rpc.h / .cpp         #   Raft 网络层
│   ├── main_namenode.cpp             #   NameNode 入口
│   ├── main_datanode.cpp             #   DataNode 入口
│   └── main_namenode_ha.cpp          #   HA NameNode 入口
├── tests/                            # 测试文件
│   ├── test_phase1.cpp               #   LSM 引擎单元测试
│   ├── test_end_to_end.cpp           #   端到端集成测试
│   ├── test_phase3_raft.cpp          #   Raft 正确性测试
│   ├── test_raft_stress.cpp          #   Raft 压力测试
│   ├── test_namenode.cpp             #   NameNode RPC 测试
│   ├── test_metadata_store.cpp       #   元数据存储测试
│   ├── test_block_store.cpp          #   块存储测试
│   ├── test_thread_pool.cpp          #   线程池测试
│   └── test_week8_fault_tolerance.cpp #   故障容错测试
└── docs/
    └── phase3_raft_blog.md           #   Raft 设计文档（中文）
```

---

## 分阶段设计

### Phase 1 — 单机 LSM-Tree 存储引擎

从零实现的 LSM-Tree 键值存储，灵感来自 LevelDB：

```
写入路径:  Put → MemTable + WAL → (MemTable 满) → Flush → SSTable
读取路径:  Get → MemTable → SSTable (新→旧) → Bloom Filter 过滤
后台任务:  Compaction (合并排序 SSTable，回收空间)
```

| 组件 | 描述 |
|------|------|
| MemTable | 跳表实现，支持 16 级，线程安全，最大 64MB |
| WAL | 32KB 块写入，每条记录 CRC32 校验 |
| SSTable | 数据块 + 索引块 + Bloom Filter + Footer |
| LRU Cache | 容量可配置，追踪命中率 |
| Compaction | 多路归并排序，生成新 SSTable |

### Phase 2 — 分布式文件系统

基于 HDFS 架构的分布式存储：

- **NameNode**：中心化元数据管理，文件系统命名空间
- **DataNode**：块级别数据存储，每块 4MB
- **Client**：透明分块、组装，对用户提供文件级 API
- **网络层**：自研 epoll 异步网络框架 + 线程池

**写入流程：**
```
Client                    NameNode                   DataNodes
  │                          │                          │
  ├─ CreateFile(/f, size) ──►│                          │
  ├─ AllocateBlock(/f, 0) ──►│                          │
  │◄── [DN0, DN1, DN2] ─────┤                          │
  │                          │                          │
  ├──── WriteBlock ──────────┼───────────────────────► DN0
  │                          │                ┌──────► DN1 (pipeline)
  │                          │                │───────► DN2 (pipeline)
  │◄── Success ──────────────┼──────────────────────────┘
```

### Phase 3 — Raft 高可用

通过 Raft 共识协议消除 NameNode 单点故障，详见 [Raft 设计文档](docs/phase3_raft_blog.md)。

---

## Raft 高可用

### 核心实现

| 机制 | 实现细节 |
|------|----------|
| Leader 选举 | 随机选举超时 2-4 秒，Term 单调递增 |
| 日志复制 | Leader 接收 `Propose()`，发送 `AppendEntries` 到 Followers |
| 提交确认 | 大多数（N/2+1）节点确认后提交，推进 `commit_index` |
| 快照 | 定期序列化 `MetadataStore` 到二进制文件，压缩 Raft 日志 |
| 持久化 | `current_term`、`voted_for`、`log_entries` 写入磁盘 |
| 快速回溯 | 冲突时二分查找日志中的匹配点，减少 AppendEntries 轮次 |

### 请求路由

```
写请求 (CreateFile, DeleteFile, AllocateBlock)
  │
  ├── 当前节点是 Leader?
  │   ├── 是 → Propose() → 复制到 Followers → 多数确认 → 应用 → 返回
  │   └── 否 → 返回 Leader ID，客户端重定向
  │
读请求 (GetFileBlocks, ListFiles)
  │
  └── 直接本地读取 MetadataStore（无需 Raft 共识）

心跳 / 块报告
  │
  └── 每个 NameNode 独立处理（不经过 Raft）
```

---

## 性能与可靠性

### 数据可靠性

| 机制 | 保障 |
|------|------|
| 3 副本复制 | 容忍 2 个 DataNode 同时故障 |
| WAL + fsync | 写入不丢失 |
| CRC32 校验 | 检测数据损坏/位翻转 |
| 一致性检查器 | 定期扫描副本 CRC32，发现不一致 |
| 自动修复 | 副本数不足时自动从健康节点复制 |
| Raft 快照 | 元数据可恢复到一致状态 |

### 设计权衡

- **读写分离**：写走 Raft 多数派，读走本地 —— 优先可用性
- **短连接 RPC**：Raft 节点间使用短 TCP 连接，简单可靠
- **快照策略**：日志超过 1000 条触发快照，平衡磁盘占用和恢复速度
- **Pipeline 复制**：DataNode 写入时顺序转发到副本节点，减少客户端上行带宽

---

## 作者

**Remedios** — 个人项目，全部代码独立完成。

## 许可证

MIT License

---

## 参考

- [HDFS Architecture Guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)
- [Raft: In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
- [LevelDB](https://github.com/google/leveldb)
- [Ceph Architecture](https://docs.ceph.com/en/latest/architecture/)
