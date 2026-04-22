#include<iostream>
#include"SSTableReader.h"
#include"coding.h"
#include"BloomFilter.h"

namespace mini_storage {

	SSTableReader::SSTableReader(const std::string& filename)
		: filename_(filename){}

	SSTableReader::~SSTableReader() {
		if (file_.is_open())
			file_.close();
	}

	bool SSTableReader::Open() {
		file_.open(filename_, std::ios::binary);
		if (!file_)
			return false;

		//1、读取最后的12字节footer
		file_.seekg(0, std::ios::end);
		size_t file_size = file_.tellg();
		if (file_size < 24)
			return false;//必须大于等于Footer长度

		char footer_buf[24];
		file_.seekg(file_size - 24);
		file_.read(footer_buf, 24);

		//2、解析索引块的位置(对应之前写入的fixed64和fixed32)
		//解析顺序必须和Builder写入顺序一致
		uint64_t index_offset = DecodeFixed64(footer_buf);
		uint32_t total_index_size = DecodeFixed32(footer_buf + 8);
		uint64_t filter_offset = DecodeFixed64(footer_buf + 12);
		uint32_t filter_size = DecodeFixed32(footer_buf + 20);


		//3、加载布隆过滤器
		if (filter_size > 0) {
			std::vector<char> filter_data(filter_size);
			file_.seekg(filter_offset);
			file_.read(filter_data.data(), filter_size);

			// 假设 BloomFilter 构造函数支持从 raw data 恢复
			filter_ = std::make_unique<BloomFilter>(std::string(filter_data.begin(), filter_data.end()));
		}

		//3、读取并解析整个索引块
		std::string full_index_data;
		full_index_data.resize(total_index_size);
		file_.seekg(index_offset);
		file_.read(&full_index_data[0], total_index_size);

		size_t index_data_size = total_index_size - 4;
		uint32_t saved_crc = DecodeFixed32(&full_index_data[index_data_size]);
		uint32_t actual_crc = ValueCRC32(0xffffffff, full_index_data.data(), index_data_size);

		if (actual_crc != saved_crc) {
			std::cerr << "[错误] 索引块 CRC 校验失败！文件可能损坏。" << std::endl;
			return false;
		}

		const char* p = full_index_data.data();
		const char* limit = p + index_data_size;
		while (p < limit) {
			std::string max_key;
			if (!DecodeString(&p, limit, &max_key))
				break;//解析key

			uint32_t offset, size;
			int n1 = DecodeVarint32(p, &offset);
			if (n1 <= 0)break;
			p += n1;//解析块偏移量
			
			int n2 = DecodeVarint32(p, &size);
			if (n2 <= 0)break;
			p += n2;//解析块大小

			index_[max_key] = { offset,size };
		}
		return true;
	}

	bool SSTableReader::Get(const std::string& key, std::string* value) {
		//关键：先问过滤器，如果过滤器说没有，直接判定不存在，剩下一次磁盘IO
		if (filter_ && !filter_->MayContain(key)) {
			return false;
		}
		//1、在内存索引中寻找可能包含该key的数据块（寻找第一个>=key的块）
		auto it = index_.lower_bound(key);
		if (it == index_.end())
			return false;

		//2、定位并读取数据块
		uint32_t block_total_size = it->second.size;
		std::string full_block_data;
		full_block_data.resize(it->second.size);
		file_.seekg(it->second.offset);
		file_.read(&full_block_data[0], it->second.size);

		//3、CRC32校验逻辑
		//A.计算纯数据的大小（总大小-4字节校验码）
		size_t block_data_size = block_total_size - 4;

		//B.从读取到的buffer末尾提取存储的CRC值
		uint32_t saved_crc = DecodeFixed32(&full_block_data[block_data_size]);

		//C.使用你提供的算法计算当前数据的CRC
		uint32_t actual_crc = ValueCRC32(0xffffffff, full_block_data.data(), block_data_size);

		//D.比对校验和
		if (actual_crc != saved_crc) {
			std::cerr << "[错误] 数据块 CRC 校验失败！块偏移量：" << it->second.offset << std::endl;
			return false;//数据孙环，直接拦截
		}

		//4、在块内线性查找
		const char* p = full_block_data.data();
		const char* limit = p + block_data_size;//这里的limit必须排除CRC字节
		while (p < limit) {
			std::string curr_key, curr_value;
			if (!DecodeString(&p, limit, &curr_key))break;
			if (!DecodeString(&p, limit, &curr_value))break;

			if (curr_key == key) {
				*value = curr_value;
				return true;
			}
		}
		return false;
	}
}//namespace mini_storage