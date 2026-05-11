#ifndef MINI_STORAGE_SSTABLE_BUILDER_H
#define MINI_STORAGE_SSTABLE_BUILDER_H

#include<string>
#include<fstream>
#include<cstdint>
#include<vector>

namespace mini_storage {
	class SSTableBuilder {
	public:
		SSTableBuilder(const std::string& filename);
		~SSTableBuilder();

		//添加键值对（外部循环调用迭代器，然后传进来）
		void Add(const std::string& key, const std::string& value);

		//完成构建：写入最后的data block、index block和footer
		bool Finish();

		//获取当前文件总大小
		size_t FileSize() const { return offset_; }

	private:
		void FlushDataBlock();//将当前缓冲区的数据写入磁盘

		std::ofstream file_;
		std::string filename_;
		std::vector<std::string>keys_;

		//对应头文件要求的变量
		std::string buffer_;//暂存当前的data block数据
		uint32_t num_entries_;//累计写入的kv条目数量

		//内部辅助变量
		uint64_t offset_;//已写入文件的总偏移量
		std::string index_block_;//累计生成的索引数据
		std::string last_key_;//当前data block的最后一个key，用于索引
	};
}//namespace mini_storage
#endif

