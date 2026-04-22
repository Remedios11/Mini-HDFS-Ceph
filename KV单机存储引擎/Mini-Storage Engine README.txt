Mini-Storage Engine: 基于 LSM-Tree 的高性能单机键值存储引擎
📖 项目简介
本项目是一个使用现代 C++ 从零实现的、基于 LSM-Tree（Log-Structured Merge-Tree） 架构的单机键值（Key-Value）存储引擎。项目整体架构与核心设计参考了业内著名的存储引擎（如 LevelDB、RocksDB 和 Pebble）。

该引擎旨在解决大规模数据的高吞吐量写入与高效读取问题，实现了数据在内存与磁盘间的高效流转，并具备完整的持久化机制与崩溃恢复能力。

🚀 核心特性与模块架构
本项目通过四个阶段的迭代构建而成，实现了从纯内存态到具备工业级可靠性的完整存储系统：

1. 高性能内存数据结构（MemTable）
底层结构：基于概率分层的 跳表（SkipList） 实现，保证平均 O(log N) 的插入、查找和删除性能。

数据有序性：内存中的 Key 始终保持有序，天然契合 LSM-Tree 批量顺序刷盘的需求。

2. 磁盘持久化存储（SSTable）
文件结构设计：实现了经典的三段式结构（Data Block -> Index Block -> Footer）。

稀疏索引：Index Block 仅存储每个 Data Block 的起始 Key 与偏移量，将内存占用降低 10~100 倍，大幅提升块定位速度。

空间优化：针对整数（如大小、偏移量）采用 Varint (变长编码)，对于大量较小的数值，可节省高达 75% 的存储空间。

二分查找：读取时先通过索引定位 Block，在 Block 内部通过二分搜索实现极速点查。

3. 数据可靠性与崩溃恢复（WAL）
Write-Ahead Log (预写日志)：实现了严格的 Write Ordering。所有写请求（Put/Delete）必须先追加写入 .log 文件并 fsync 落盘，再写入 MemTable。

原子操作：实现了 WriteBatch 机制，支持多条写入操作的原子提交。

崩溃恢复 (Recover)：系统重启时，自动读取未持久化的 WAL 文件并进行幂等回放，保证进程意外崩溃或断电情况下的数据零丢失。

4. 读取加速与系统维护（Read Path & Compaction）
Bloom Filter (布隆过滤器)：在读取 SSTable 前进行拦截过滤。利用多个哈希函数（如 MurmurHash3）快速判断 Key 是否存在，极大程度避免了无效的磁盘 I/O。

CRC32 校验：对落盘的数据块进行 CRC32 校验，防止数据静默损坏，保障数据完整性。

LRU Cache (最近最少使用缓存)：针对热点数据，在内存中缓存最近访问过的 Data Block，大幅提升“热读”的 QPS。

数据压缩与合并 (Compaction)：后台自动合并底层的多个 SSTable 文件，清理被标记为删除（Tombstone）的数据和冗余的历史版本，保持良好的读取性能并控制磁盘空间膨胀。

🛠️ 技术栈
开发语言：C++ (C++11/C++17)

核心组件：SkipList, Hash Table, Double Linked List

并发控制：std::mutex, std::lock_guard

文件操作：std::fstream, std::filesystem API

📂 存储目录结构示意
Plaintext
db_data/
├── 000001.log       # 活跃的 Write-Ahead Log，记录最新的写入
├── 000002.sst       # 刷盘生成的持久化文件 (SSTable)
├── 000003.sst       # 刷盘生成的持久化文件 (SSTable)
└── MANIFEST         # 元数据清单，记录当前有效的日志和数据文件状态
💡 写入与读取流水线 (I/O Path)
Write Path（写路径）：
Client 写入 -> 追加写入 WAL 并 fsync -> 写入内存 MemTable -> 返回成功
(当 MemTable 达到阈值如 4MB 时，冻结并异步转存为磁盘上的 SSTable 文件)

Read Path（读路径）：
Client 读取 -> 查询活跃 MemTable -> 查询只读 MemTable -> 查询 LRU Block Cache -> 布隆过滤器拦截 -> 读取 SSTable Index -> 加载对应 Data Block 并二分查找
