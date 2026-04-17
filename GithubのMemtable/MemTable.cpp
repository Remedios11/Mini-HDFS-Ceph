#include "MemTable.h"
#include<ctime>

namespace mini_storage {
    // 构造函数：初始化头节点(key/value为空，层数为最大)
    MemTable::MemTable()
        : head_(new SkipListNode("", "", MAX_LEVEL)),
        level_count_(1),
        count_(0),
        size_(0),
        rng_(std::time(0)) {}

    // 析构函数：释放链表所有节点
    MemTable::~MemTable() {
        Clear();
        delete head_;
    }

    //随机层数产生器：P=0.25的概率增加一层，最大不超过MAX_LEVEL
    int MemTable::RandomLevel() {
        int level = 1;
        //使用均匀分布随机数
        std::uniform_real_distribution<double>dist(0, 1);
        while (dist(rng_) < 0.25 && level < MAX_LEVEL) {
            level++;
        }
        return level;
    }

    //插入/更新数据(Put)
    void MemTable::Put(const std::string& key, const std::string& value) {
        std::lock_guard<std::mutex> lock(mutex_);//物理锁保证并发安全

        //用于记录每一层中，目标位置的前驱节点
        std::vector<SkipListNode*>update(MAX_LEVEL, nullptr);
        SkipListNode* curr = head_;

        //1.从最高层向下寻找插入位置
        for (int i = level_count_ - 1; i >= 0; i--) {
            while (curr->next[i] != nullptr && curr->next[i]->key < key) {
                curr = curr->next[i];
            }
            update[i] = curr;
        }

        //检查最底层的下一个节点是否就是我们要找的key
        curr = curr->next[0];

        if (curr != nullptr && curr->key == key) {
            //情况A：key已存在，执行更新操作
            //先减去旧value的大小
            size_ -= curr->value.size();
            curr->value = value;
            //加上新value的大小
            size_ += value.size();
        }
        else {
            //情况B：key不存在，插入新节点
            int new_level = RandomLevel();

            //如果新节点层数超过当前跳表高度，更新高层的前驱为头节点
            if (new_level > level_count_) {
                for (int i = level_count_; i < new_level; i++) {
                    update[i] = head_;
                }
                level_count_ = new_level;
            }

            //创建并插入节点
            SkipListNode* newNode = new SkipListNode(key, value, new_level);
            for (int i = 0; i < new_level; i++) {
                newNode->next[i] = update[i]->next[i];
                update[i]->next[i] = newNode;
            }

            //更新统计信息
            count_++;
            size_ += (key.size() + value.size());
        }
    }
    //读取数据(Get)
    bool MemTable::Get(const std::string& key, std::string* value) {
        std::lock_guard<std::mutex>lock(mutex_);
        SkipListNode* curr = head_;

        //从顶层向下查找，利用跳表的索引实现快速定位
        for (int i = level_count_ - 1; i >= 0; i--) {
            while (curr->next[i] != nullptr && curr->next[i]->key < key) {
                curr = curr->next[i];
            }
        }
        curr = curr->next[0];//移向最底层可能的匹配节点
        if (curr != nullptr && curr->key == key) {
            if (value)*value = curr->value;
            return true;
        }
        return false;
    }

    //返回占用内存大小
    size_t MemTable::Size() const {
        std::lock_guard<std::mutex>lock(mutex_);
        return size_;
    }

    //是否为空
    bool MemTable::Empty() const {
        std::lock_guard<std::mutex>lock(mutex_);
        return count_ == 0;
    }
    
    //清空所有数据
    void MemTable::Clear() {
        std::lock_guard<std::mutex>lock(mutex_);
        //从最底层的链表开始，逐个释放节点内存
        SkipListNode* curr = head_->next[0];
        while (curr != nullptr) {
            SkipListNode* next = curr->next[0];
            delete curr;
            curr = next;
        }

        //重置头节点的指针数组
        for (int i = 0; i < MAX_LEVEL; i++) {
            head_->next[i] = nullptr;
        }

        size_ = 0;
        count_ = 0;
        level_count_ = 1;
    }

    //计算内存占用（辅助方法）
    size_t MemTable::CalculateSize(const std::string& key, const std::string& value) {
        return key.size() + value.size();
    }

    //迭代器接口实现
    
    //创建新迭代器
    MemTable::Iterator* MemTable::NewIterator() {
        //注意：在实际工业级实现中，这里通常会加锁或使用快照
        return new Iterator(head_);
    }

    //迭代器构造函数：跳表迭代器只需要从最底层的第一个节点开始即可
    MemTable::Iterator::Iterator(SkipListNode* head) {
        current_ = head->next[0];//跳表的level0就是一个有序单链表
    }

    bool MemTable::Iterator::Valid() const {
        return current_ != nullptr;
    }

    void MemTable::Iterator::Next() {
        if (Valid()) {
            current_ = current_->next[0];
        }
    }

    std::string MemTable::Iterator::Key() const {
        return current_->key;
    }

    std::string MemTable::Iterator::Value() const {
        return current_->value;
    }
} // namespace mini_storage