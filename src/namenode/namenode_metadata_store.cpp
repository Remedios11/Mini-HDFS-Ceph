#include "namenode_metadata_store.h"
#include <chrono>
#include <algorithm>
#include <mutex>

namespace mini_storage {

static int64_t NowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

bool MetadataStore::CreateFile(const FilePath& path, FileInfo* out_info) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (files_.count(path)) return false;

    FileInfo info;
    info.path        = path;
    info.size        = 0;
    info.create_time = NowMs();
    info.modify_time = info.create_time;

    files_[path] = info;
    if (out_info) *out_info = info;
    return true;
}

std::optional<FileInfo> MetadataStore::GetFile(const FilePath& path) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = files_.find(path);
    if (it == files_.end()) return std::nullopt;
    return it->second;
}

bool MetadataStore::UpdateFile(const FilePath& path, const FileInfo& info) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = files_.find(path);
    if (it == files_.end()) return false;
    it->second = info;
    it->second.modify_time = NowMs();
    return true;
}

bool MetadataStore::DeleteFile(const FilePath& path) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    return files_.erase(path) > 0;
}

std::vector<FileInfo> MetadataStore::ListFiles(const std::string& dir_prefix) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    std::vector<FileInfo> result;
    for (const auto& [path, info] : files_) {
        if (path.find(dir_prefix) == 0) result.push_back(info);
    }
    return result;
}

void MetadataStore::AddBlock(const BlockInfo& block) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    blocks_[block.block_id] = block;
}

std::optional<BlockInfo> MetadataStore::GetBlock(BlockID block_id) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = blocks_.find(block_id);
    if (it == blocks_.end()) return std::nullopt;
    return it->second;
}

bool MetadataStore::UpdateBlock(const BlockInfo& block) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = blocks_.find(block.block_id);
    if (it == blocks_.end()) return false;
    it->second = block;
    return true;
}

bool MetadataStore::DeleteBlock(BlockID block_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    return blocks_.erase(block_id) > 0;
}

void MetadataStore::UpdateBlockLocation(BlockID block_id, const DataNodeID& dn_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto& block = blocks_[block_id];
    block.block_id = block_id;
    auto& locs = block.locations;
    if (std::find(locs.begin(), locs.end(), dn_id) == locs.end()) {
        locs.push_back(dn_id);
    }
}

std::vector<DataNodeID> MetadataStore::GetBlockLocations(BlockID block_id) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = blocks_.find(block_id);
    if (it == blocks_.end()) return {};
    return it->second.locations;
}

size_t MetadataStore::FileCount() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return files_.size();
}

size_t MetadataStore::BlockCount() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return blocks_.size();
}

}  // namespace mini_storage
