#include"src_wal_log_writer.h"
#include"coding.h"
#include<cstring>
#include<iostream>

namespace mini_storage {
	LogWriter::LogWriter(const std::string& filename)
		:filename_(filename), ok_(true), block_offset_(0) {
        //修改点：使用out|app确保文件不存在时创建，存在时追加
        file_.open(filename, std::ios::binary | std::ios::out | std::ios::app);
        
        if (!file_.is_open()) {
            std::cerr << "[LogWriter] Failed to open: " << filename << std::endl;
            ok_ = false;
        }
	}

    LogWriter::~LogWriter() {
        if (file_.is_open()) {
            file_.flush();
            file_.close();
        }
    }

	bool LogWriter::AddRecord(const std::string& data) {
		if (!ok_) return false;
		return EmitPhysicalRecord(kFullType, data.data(), data.size());
	}

    bool LogWriter::EmitPhysicalRecord(RecordType type,
        const char* data,
        size_t length) {
        // 构建 Header
        // 格式：[CRC32(4字节)][Length(2字节)][Type(1字节)]
        char header[kHeaderSize];

        // 先填 Length 和 Type（CRC 要覆盖这两个字段）
        header[4] = static_cast<char>(length & 0xff);
        header[5] = static_cast<char>(length >> 8);
        header[6] = static_cast<char>(type);

        // 计算 CRC32：覆盖 type + data
        uint32_t crc = ValueCRC32(0xFFFFFFFF,header + 6, 1);// type
        crc = ValueCRC32(crc, data, length);// data
        EncodeFixed32(header, crc);

        // 写入 Header
        file_.write(header, kHeaderSize);
        // 写入数据
        file_.write(data, length);

        ok_ = file_.good();
        return ok_;
    }

    bool LogWriter::Sync() {
        file_.flush();
        return file_.good();
    }

}  // namespace mini_storage