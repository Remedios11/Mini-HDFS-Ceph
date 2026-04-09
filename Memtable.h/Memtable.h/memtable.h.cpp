#ifndef MINI_STORAGE_MEMTABLE_H
#define MINI_STORAGE_MEMTABLE_H

#include <string>
#include <map>
#include <mutex>

namespace mini_storage {

    class MemTable {
    public:
        MemTable();
        ~MemTable();

        // 核心操作
        void Put(const std::string& key, const std::string& value);
        bool Get(const std::string& key, std::string* value);

        // 工具方法
        size_t Size() const;  // 返回占用内存大小（字节）
        bool Empty() const;   // 是否为空
        void Clear();         // 清空数据

        // 迭代器（后续刷盘时用）
        class Iterator {
        public:
            bool Valid() const;
            void Next();
            std::string Key() const;
            std::string Value() const;

        private:
            friend class MemTable;
            Iterator(const std::map<std::string, std::string>* data);
            std::map<std::string, std::string>::const_iterator iter_;
            std::map<std::string, std::string>::const_iterator end_;
        };

        Iterator* NewIterator();

    private:
        std::map<std::string, std::string> data_;  // 实际存储
        size_t size_;  // 当前占用内存大小
        mutable std::mutex mutex_;  // 保护并发访问

        // 计算key-value占用的内存
        size_t CalculateSize(const std::string& key, const std::string& value);
    };

} // namespace mini_storage

#endif