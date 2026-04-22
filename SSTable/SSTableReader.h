#ifndef  MINI_STORAGE_SSTABLE_READER_H
#define  MINI_STORAGE_SSTABLE_READER_H

#include<string>
#include<map>
#include<fstream>
#include<memory>
#include"BloomFilter.h"

namespace mini_storage {

	class SSTableReader {
	public:
		SSTableReader(const std::string& filename);
		~SSTableReader();

		//打开文件并加载索引块
		bool Open();

		//根据key查找value
		bool Get(const std::string& key, std::string* value);

	private:
		std::string filename_;
		std::ifstream file_;
		std::unique_ptr<BloomFilter>filter_;

		//索引：记录每个块的最大key->该块在文件中的文件位置
		struct BlockHandle {
			uint64_t offset;
			uint32_t size;
		};
		std::map<std::string, BlockHandle>index_;
	};//namespace mini_storage
}
#endif
