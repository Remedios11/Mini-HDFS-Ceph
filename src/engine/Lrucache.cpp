#include"Lrucache.h"
#include<stdexcept>

namespace mini_storage {

	LRUCache::LRUCache(size_t capacity)
		:capacity_(capacity), hits_(0), misses_(0) {
		if (capacity_ == 0)capacity_ = 1;

		//创建哨兵节点，简化链表边界处理
		head_ = new Node("__head__", {});
		tail_ = new Node("__tail__", {});
		head_->next = tail_;
		tail_->prev = head_;
	}

	LRUCache::~LRUCache() {
		//释放所有业务节点
		Node* cur = head_->next;
		while (cur != tail_) {
			Node* next = cur->next;
			delete cur;
			cur = next;
		}
		//释放哨兵节点
		delete head_;
		delete tail_;
	}
	
	// Get：查缓存

	bool LRUCache::Get(const std::string& cache_key, BlockData* out) {
		std::lock_guard<std::mutex> lock(mutex_);

		auto it = cache_map_.find(cache_key);
		if (it == cache_map_.end()) {
			misses_++;
			return false;//未命中
		}
		MoveToFront(it->second);
		*out = it->second->data;
		hits_++;
		return true;
	}

	// Insert：插入缓存
	void LRUCache::Insert(const std::string& cache_key, const BlockData& data) {
		std::lock_guard<std::mutex> lock(mutex_);

		//如果key已存在，更新数据并移到头部
		auto it = cache_map_.find(cache_key);
		if (it != cache_map_.end()) {
			it->second->data = data;
			MoveToFront(it->second);
			return;
		}
		// 容量满了，先淘汰最久未用的（尾部）
		if(cache_map_.size() >= capacity_) {
			Exict();
		}
		
		//新节点插入头部
		Node* node = new Node(cache_key, data);
		InsertToFront(node);
		cache_map_[cache_key] = node;
	}

	//链表操作
	void LRUCache::MoveToFront(Node* node) {
		RemoveFromList(node);
		InsertToFront(node);
	}

	void LRUCache::InsertToFront(Node* node) {
		//插入到 head_ 后面
		node->prev = head_;
		node->next = head_->next;
		head_->next->prev = node;
		head_->next = node;
	}

	void LRUCache::RemoveFromList(Node* node) {
		node->prev->next = node->next;
		node->next->prev = node->prev;
		node->prev = nullptr;
		node->next = nullptr;
	}

	void LRUCache::Exict() {
		//淘汰 tail_->prev（最久未用的节点）
		Node* lru = tail_->prev;
		if (lru == head_)return;//链表为空，不淘汰

		RemoveFromList(lru);
		cache_map_.erase(lru->key);
		delete lru;
	}

	//统计信息
	size_t LRUCache::Size() const {
		std::lock_guard<std::mutex> lock(mutex_);
		return cache_map_.size();
	}

	size_t LRUCache::Capacity() const {
		return capacity_;
	}

	uint64_t LRUCache::Hits() const {
		std::lock_guard<std::mutex> lock(mutex_);
		return hits_;
	}

	uint64_t LRUCache::Misses() const {
		std::lock_guard<std::mutex> lock(mutex_);
		return misses_;
	}

	double LRUCache::HitRate() const {
		std::lock_guard<std::mutex> lock(mutex_);
		uint64_t total = hits_ + misses_;
		if (total == 0)return 0.0;
		return static_cast<double>(hits_) / total * 100.0;
	}

	void LRUCache::Clear() {
		std::lock_guard<std::mutex> lock(mutex_);
		//清空链表
		Node* cur = head_->next;
		while (cur != tail_) {
			Node* next = cur->next;
			delete cur;
			cur = next;
		}
		head_->next = tail_;
		tail_->prev = head_;
		cache_map_.clear();
		hits_ = 0;
		misses_ = 0;
	}
}//namespace mini_storage