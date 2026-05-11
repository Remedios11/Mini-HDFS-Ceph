#ifndef MINI_STORAGE_BLOOM_FILTER_H
#define MINI_STORAGE_BLOOM_FILTER_H

#include<vector>
#include<string>
#include<cmath>
#include"coding.h"

namespace mini_storage {

	class BloomFilter {
	public:
		//bits_per_key:每个key分配多少位（通常10位能达到1%报错率）
		BloomFilter(int entries, int bits_per_key = 10) {
			size_t bits = entries * bits_per_key;
			if (bits < 64)bits = 64;
			bits_ = ((bits + 7) / 8) * 8;//按字节对齐
			data_.assign(bits_ / 8, 0);
			num_hashes_ = static_cast<int>(bits_per_key * 0.69);//k=ln2*m/n
			if (num_hashes_ < 1)
				num_hashes_ = 1;
		}

		//从磁盘数据恢复过滤器
		BloomFilter(const std::string& raw_data) {
			if (raw_data.empty()) {
				bits_ = 0; num_hashes_ = 1; return;
			}
			num_hashes_ = static_cast<int>(static_cast<unsigned char>(raw_data[0]));
			data_.assign(raw_data.begin() + 1, raw_data.end());
			bits_ = data_.size() * 8;
			//简单固定哈希次数或从元数据读取，这里示例固定使用7
		}

		void Add(const std::string& key) {
			uint32_t h = ValueCRC32(0, key.data(), key.size());
			for (int i = 0; i < num_hashes_; i++) {
				size_t bit_pos = h % bits_;
				data_[bit_pos / 8] |= (1 << (bit_pos % 8));
				h = ValueCRC32(h, key.data(), key.size());//简单迭代哈希
			}
		}

		bool MayContain(const std::string& key)const {
			if (data_.empty())return true;
			uint32_t h = ValueCRC32(0, key.data(), key.size());
			for (int i = 0; i < num_hashes_; i++) {
				size_t bit_pos = h % bits_;
				if (!(data_[bit_pos / 8] & (1 << (bit_pos % 8)))) {
					return false;//绝对不存在
				}
				h = ValueCRC32(h, key.data(), key.size());
			}
			return true;//可能存在
		}

		const std::string& Data()const {
			static std::string res;
			res.assign(data_.begin(), data_.end());
			return res;
		}

		const std::vector<unsigned char>& RawData()const { return data_; }
		
		int NumHashes()const { return num_hashes_; }
	
	private:
		size_t bits_;
		int num_hashes_;
		std::vector<unsigned char>data_;
	};
}
#endif //namespace mini_storage

