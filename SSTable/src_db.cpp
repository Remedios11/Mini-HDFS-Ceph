#include "src_db.h"
#include "src_wal_log_reader.h"
#include "SSTableBuilder.h"
#include <iostream>
#include <filesystem>

namespace mini_storage {

    class MemTableInserter : public WriteBatch::Handler {
    public:
        MemTable* mem_;
        void Put(const std::string& key, const std::string& value) override {
            mem_->Put(key, value);
        }
        void Delete(const std::string& key) override {
            mem_->Put(key, "");  // 标记为删除
        }
    };

    DB::DB(const Options& options)
        : options_(options), next_log_id_(0), mem_table_(nullptr), log_writer_(nullptr) {

        std::filesystem::create_directories(options_.db_path);
        mem_table_ = new MemTable();

        Recover();
        log_writer_ = new LogWriter(GenerateLogFilename());
    }

    DB::~DB() {
        delete log_writer_;
        delete mem_table_;
    }

    void DB::Recover() {
        std::string log_filename = GenerateLogFilename();
        if (!std::filesystem::exists(log_filename)) return;

        LogReader reader(log_filename);
        std::string record;
        MemTableInserter inserter;
        inserter.mem_ = mem_table_;

        int count = 0;
        while (reader.ReadRecord(&record)) {
            WriteBatch batch;
            batch.SetContents(record);
            if (batch.Iterate(&inserter)) {
                count++;
            }
            else {
                std::cerr << "[DB] 记录解析失败，数据可能错位！" << std::endl;
            }
        }
        if (count > 0) std::cout << "[DB] 恢复成功，回放了 " << count << " 条 Batch 记录。" << std::endl;
    }

    std::string DB::GenerateLogFilename() {
        return options_.db_path + "/current.log";
    }

    bool DB::Put(const std::string& key, const std::string& value) {
        std::lock_guard<std::mutex> lock(mutex_);

        WriteBatch batch;
        batch.Put(key, value);

        if (!log_writer_->AddRecord(batch.Data())) return false;
        log_writer_->Sync();

        mem_table_->Put(key, value);

        if (mem_table_->Size() >= options_.write_buffer_size) {
            FlushMemTable();
        }
        return true;
    }

    bool DB::Get(const std::string& key, std::string* value) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (mem_table_->Get(key, value)) {
            // ✅ 关键修复：如果值为空字符串，说明 key 被删除了
            if (value->empty()) {
                return false;
            }
            return true;
        }
        return false;
    }

    bool DB::Delete(const std::string& key) {
        std::lock_guard<std::mutex> lock(mutex_);
        WriteBatch batch;
        batch.Delete(key);
        if (!log_writer_->AddRecord(batch.Data())) return false;
        mem_table_->Put(key, "");
        return true;
    }

    void DB::FlushMemTable() {
        if (mem_table_->Empty()) return;
        std::string sst_name = options_.db_path + "/data.sst";
        SSTableBuilder builder(sst_name);
        auto* iter = mem_table_->NewIterator();
        for (; iter->Valid(); iter->Next()) {
            builder.Add(iter->Key(), iter->Value());
        }
        builder.Finish();
        delete iter;

        mem_table_->Clear();
        delete log_writer_;
        std::filesystem::remove(GenerateLogFilename());
        log_writer_ = new LogWriter(GenerateLogFilename());
    }
}//namespace mini_storage