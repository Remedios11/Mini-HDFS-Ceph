# 🚀 Mini Distributed Storage System

分布式存储系统

📋 项目简介

Mini Distributed Storage System 是一个参考 HDFS、Ceph、RocksDB 设计的分布式存储系统，旨在通过实战学习：

🔥 LSM-Tree 存储引擎：高性能的写入优化存储

🌐 分布式架构：Master-Slave 模式、心跳检测、故障恢复

⚡ 高并发处理：Reactor 网络模型、线程池、零拷贝

🛡️ 数据可靠性：多副本机制、一致性保证、CRC 校验

🎯 共识协议：Raft 实现、主从切换、日志复制

🎯 第一阶段：单机存储引擎 (Week 1-4)

功能	          描述	                状态

MemTable	      内存表（跳表实现）	    🔨 开发中

SSTable	        磁盘数据文件	        ⏳ 计划中

WAL	            写前日志（崩溃恢复）	  ⏳ 计划中

Compaction	    数据压缩合并	        ⏳ 计划中

Bloom Filter	  快速查找优化	        ⏳ 计划中

技术亮点：

LSM-Tree 架构（写入优化）

跳表实现 O(log n) 查询

CRC32 数据校验

LRU 缓存机制
