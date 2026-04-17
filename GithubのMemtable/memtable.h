#ifndef MINI_STORAGE_MEMTABLE_H
#define MINI_STORAGE_MEMTABLE_H//防止同一个头文件被重复包含多次

#include <string>
#include <vector>
#include <mutex>
#include<random>

namespace mini_storage {

    //跳表最大高度
    const int MAX_LEVEL = 16;

    //跳表节点定义
    struct SkipListNode {
        std::string key;
        std::string value;
        //存储每一层的前向指针
        std::vector<SkipListNode*>next;

        SkipListNode(std::string k, std::string v, int level)
            :key(std::move(k)), value(std::move(v)), next(level, nullptr) {}
    };
    class MemTable {
    public:
        MemTable();//构造函数
        ~MemTable();//析构函数

        // 核心操作
        void Put(const std::string& key, const std::string& value);//存入数据
        bool Get(const std::string& key, std::string* value);//读取数据

        // 工具方法
        size_t Size() const;  // 返回占用内存大小（字节）
        bool Empty() const;   // 是否为空
        void Clear();         // 清空数据

        size_t Count() const {
            std::lock_guard<std::mutex> lock(mutex_);
            return count_;//统计数据条数
        }
        // 迭代器（后续刷盘时用）
        class Iterator {
        public:
            Iterator(SkipListNode* head);
            bool Valid() const;
            void Next();
            std::string Key() const;
            std::string Value() const;

        private:
            SkipListNode* current_;
        };
            Iterator* NewIterator();
        private:
            //辅助方法
            int RandomLevel();
            size_t CalculateSize(const std::string& key, const std::string& value);

            //跳表成员变量
            SkipListNode* head_;//跳表头节点
            int level_count_;//当前跳表的最大层数
            size_t count_;//数据条数
            size_t size_;//内存占用大小

            mutable std::mutex mutex_;

        //随机数生成（用于决定新节点高度）
        std::mt19937 rng_;
    };

} // namespace mini_storage

#endif