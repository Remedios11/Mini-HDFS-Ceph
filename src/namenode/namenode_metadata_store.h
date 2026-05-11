#pragma once
#include "common_types.h"
#include <unordered_map>
#include <shared_mutex>
#include <optional>
#include <vector>

namespace mini_storage {

class MetadataStore {
public:
    MetadataStore() = default;

    // File operations
    bool CreateFile(const FilePath& path, FileInfo* out_info);
    std::optional<FileInfo> GetFile(const FilePath& path) const;
    bool UpdateFile(const FilePath& path, const FileInfo& info);
    bool DeleteFile(const FilePath& path);
    std::vector<FileInfo> ListFiles(const std::string& dir_prefix) const;

    // Block operations
    void AddBlock(const BlockInfo& block);
    std::optional<BlockInfo> GetBlock(BlockID block_id) const;
    bool UpdateBlock(const BlockInfo& block);
    bool DeleteBlock(BlockID block_id);

    // Week 7: Update block location from BlockReport
    void UpdateBlockLocation(BlockID block_id, const DataNodeID& dn_id);
    std::vector<DataNodeID> GetBlockLocations(BlockID block_id) const;

    size_t FileCount() const;
    size_t BlockCount() const;

private:
    std::unordered_map<FilePath, FileInfo>  files_;
    std::unordered_map<BlockID, BlockInfo>  blocks_;
    mutable std::shared_mutex               mutex_;
};

}  // namespace mini_storage
