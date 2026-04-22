#pragma once
#include<string>
#include<memory>
#include<mutex>
#include"MemTable.h"
#include"src_db_write_batch.h"
#include"src_wal_log_format.h"
#include"src_wal_log_writer.h"
#include"Lrucache.h"

namespace mini_storage {
	struct Options {
		size_t write_buffer_size = 4 * 1024 * 1024;//默认4MB
		std::string db_path = "./data";
		int max_sstable_count = 4;//超过这个数量触发compaction
		size_t block_cache_size = 128;//LRU Cache的容量，默认最多缓存128个Block
	};

	class DB {
	public:
		explicit DB(const Options& options);
		~DB();

		//禁止拷贝
		DB(const DB&) = delete;
		DB& operator=(const DB&) = delete;

		//核心接口
		bool Put(const std::string& key, const std::string& value);
		bool Delete(const std::string& key);
		bool Get(const std::string& key, std::string* value);
		void FlushMemTable();//手动触发刷盘(供测试使用)
		void Compaction();//手动触发compaction(供测试使用)

		//缓存统计（供Benchmark打印命中率）
		LRUCache* GetCache() { return block_cache_; }

	private:
		
		void MaybeScheduleFlush();

		//WAL相关
		void Recover();//从WAL恢复数据到MemTable
		std::string GenerateLogFilename();

		//SSTable相关
		std::string GenerateSSTableFilename();   // 生成带编号的文件名
		void LoadSSTableList();                  // 启动时扫描已有 .sst 文件
		bool GetFromSSTables(const std::string& key, std::string* value); // 查所有SST

		// Compaction 实现
		void DoCompaction();
		void MaybeScheduleCompaction();

		Options options_;
		uint64_t next_log_id_;
		uint64_t next_sst_id_;//下一个SSTable编号（自增）
		MemTable* mem_table_;
		LogWriter* log_writer_;
		LRUCache* block_cache_;//DB持有并管理Cache生命周期
		std::mutex mutex_;//保证多线程安全

		//已有的SSTable文件列表，从旧到新排列，查找时从后往前
		std::vector<std::string>sst_files_;

	};
}//namespace mini_storage