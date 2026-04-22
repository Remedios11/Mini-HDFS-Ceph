#pragma once
#include<string>
#include<memory>
#include<mutex>
#include"MemTable.h"
#include"src_db_write_batch.h"
#include"src_wal_log_format.h"
#include"src_wal_log_writer.h"

namespace mini_storage {
	struct Options {
		size_t write_buffer_size = 4 * 1024 * 1024;//默认4MB
		std::string db_path = "./data";
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

	private:
		void MaybeScheduleFlush();
		void FlushMemTable();
		void Recover();//从WAL恢复数据到MemTable

		Options options_;
		MemTable* mem_table_;
		LogWriter* log_writer_;
		uint64_t next_log_id_;
		std::mutex mutex_;//保证多线程安全
		std::string GenerateLogFilename();
	};
}//namespace mini_storage