#pragma once
#include<cstdint>

namespace mini_storage {
	//每条WAL记录的header大小
	//CRC(4字节)+Length(2字节)+Type(1字节)=7字节
	static const int kHeaderSize = 7;

	//WAL块大小(参考levelDB，32kb)
	//把文件分成固定大小的块，方便读取
	static const int kBlockSize = 32768;//32*1024

	//操作类型
	enum RecordType :uint8_t {
		kZeroType = 0,//预留
		kFullType = 1,//完整记录（最常见）
		kFirstType = 2,//大记录的第一块（暂时不实现）
		kMiddleType = 3,//大记录的中间快（暂时不实现）
		kLastType = 4,//大记录的最后一块（暂时不实现）
	};

	//WriteBatch内部的操作类型
	enum ValueType :uint8_t {
		kTypeDeletion = 0,//删除操作
		kTypeValue = 1,//写入操作
	};
}//namespace mini_storage