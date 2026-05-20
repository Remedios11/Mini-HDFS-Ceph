#include "datanode_block_store.h"
#include <filesystem>
#include <fstream>
#include <sstream>
#include <sys/statvfs.h>
#include <zlib.h>
#include <chrono>
#include <cstdio>

namespace fs = std::filesystem;
namespace mini_storage {

static int64_t NowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

BlockStore::BlockStore(const std::string& data_dir)
    : data_dir_(data_dir),
      blocks_dir_(data_dir + "/blocks"),
      tmp_dir_(data_dir + "/tmp") {
    fs::create_directories(blocks_dir_);
    fs::create_directories(tmp_dir_);
    CleanTempFiles();
}

bool BlockStore::WriteBlock(BlockID block_id, const std::string& data) {
    std::string tmp_path = BlockTmpPath(block_id);
    {
        std::ofstream f(tmp_path, std::ios::binary);
        if (!f.is_open()) return false;
        f.write(data.data(), (std::streamsize)data.size());
        f.flush();
    }

    BlockMeta meta;
    meta.block_id    = block_id;
    meta.size        = (int64_t)data.size();
    meta.crc32       = ComputeCRC32(data);
    meta.create_time = NowMs();
    if (!WriteMeta(block_id, meta)) {
        fs::remove(tmp_path);
        return false;
    }

    std::string dat_path = BlockDataPath(block_id);
    if (std::rename(tmp_path.c_str(), dat_path.c_str()) != 0) {
        fs::remove(tmp_path);
        return false;
    }
    return true;
}

bool BlockStore::ReadBlock(BlockID block_id, int64_t offset, int64_t length,
                            std::string* data_out) {
    BlockMeta meta;
    if (!ReadMeta(block_id, &meta)) return false;

    std::ifstream f(BlockDataPath(block_id), std::ios::binary);
    if (!f.is_open()) return false;

    int64_t file_size = meta.size;
    if (offset < 0 || offset > file_size) return false;
    if (length <= 0 || offset + length > file_size)
        length = file_size - offset;
    if (length == 0) { data_out->clear(); return true; }

    f.seekg(offset);
    data_out->resize((size_t)length);
    f.read(&(*data_out)[0], length);
    if (f.gcount() != length) return false;

    // CRC check on full read
    if (offset == 0 && length == file_size) {
        uint32_t actual = ComputeCRC32(*data_out);
        if (actual != meta.crc32) return false;
    }
    return true;
}

bool BlockStore::DeleteBlock(BlockID block_id) {
    bool ok = true;
    auto dat  = BlockDataPath(block_id);
    auto meta = BlockMetaPath(block_id);
    if (fs::exists(dat))  ok &= (bool)fs::remove(dat);
    if (fs::exists(meta)) ok &= (bool)fs::remove(meta);
    return ok;
}

bool BlockStore::HasBlock(BlockID block_id) const {
    return fs::exists(BlockDataPath(block_id));
}

std::optional<BlockMeta> BlockStore::GetBlockMeta(BlockID block_id) const {
    BlockMeta m;
    if (!ReadMeta(block_id, &m)) return std::nullopt;
    return m;
}

std::vector<BlockMeta> BlockStore::ListAllBlocks() const {
    std::vector<BlockMeta> result;
    if (!fs::exists(blocks_dir_)) return result;
    for (const auto& entry : fs::directory_iterator(blocks_dir_)) {
        std::string name = entry.path().filename().string();
        if (name.size() > 5 && name.substr(name.size() - 5) == ".meta") {
            // block_000001.meta → id = "000001"
            if (name.size() < 12) continue;  // "block_" + 6 + ".meta" = 16
            std::string id_str = name.substr(6, name.size() - 11);
            try {
                BlockID id = std::stoull(id_str);
                BlockMeta m;
                if (ReadMeta(id, &m)) result.push_back(m);
            } catch (...) {}
        }
    }
    return result;
}

int64_t BlockStore::GetFreeSpace() const {
    struct statvfs stat;
    if (statvfs(data_dir_.c_str(), &stat) != 0) return 0;
    return (int64_t)stat.f_bavail * (int64_t)stat.f_frsize;
}

void BlockStore::CleanTempFiles() {
    if (!fs::exists(tmp_dir_)) return;
    for (const auto& entry : fs::directory_iterator(tmp_dir_))
        fs::remove(entry.path());
}

std::string BlockStore::BlockDataPath(BlockID id) const {
    char buf[64];
    snprintf(buf, sizeof(buf), "block_%06llu.dat", (unsigned long long)id);
    return blocks_dir_ + "/" + buf;
}

std::string BlockStore::BlockMetaPath(BlockID id) const {
    char buf[64];
    snprintf(buf, sizeof(buf), "block_%06llu.meta", (unsigned long long)id);
    return blocks_dir_ + "/" + buf;
}

std::string BlockStore::BlockTmpPath(BlockID id) const {
    char buf[64];
    snprintf(buf, sizeof(buf), "block_%06llu.dat.tmp", (unsigned long long)id);
    return tmp_dir_ + "/" + buf;
}

bool BlockStore::WriteMeta(BlockID id, const BlockMeta& meta) {
    std::ofstream f(BlockMetaPath(id));
    if (!f.is_open()) return false;
    f << meta.block_id << " " << meta.size << " "
      << meta.crc32 << " " << meta.create_time << "\n";
    return true;
}

bool BlockStore::ReadMeta(BlockID id, BlockMeta* meta) const {
    std::ifstream f(BlockMetaPath(id));
    if (!f.is_open()) return false;
    f >> meta->block_id >> meta->size >> meta->crc32 >> meta->create_time;
    return !f.fail();
}

uint32_t BlockStore::ComputeCRC32(const std::string& data) {
    return crc32(0, (const Bytef*)data.data(), data.size());
}

}  // namespace mini_storage
