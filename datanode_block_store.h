#pragma once
#include "common_types.h"
#include <string>
#include <vector>
#include <optional>

namespace mini_storage {

struct BlockMeta {
    BlockID  block_id    = 0;
    int64_t  size        = 0;
    uint32_t crc32       = 0;
    int64_t  create_time = 0;
};

class BlockStore {
public:
    explicit BlockStore(const std::string& data_dir);

    bool WriteBlock(BlockID block_id, const std::string& data);
    bool ReadBlock(BlockID block_id, int64_t offset, int64_t length,
                   std::string* data_out);
    bool DeleteBlock(BlockID block_id);
    bool HasBlock(BlockID block_id) const;

    std::optional<BlockMeta>  GetBlockMeta(BlockID block_id) const;
    std::vector<BlockMeta>    ListAllBlocks() const;
    int64_t                   GetFreeSpace() const;
    void                      CleanTempFiles();

private:
    std::string data_dir_;
    std::string blocks_dir_;
    std::string tmp_dir_;

    std::string BlockDataPath(BlockID id) const;
    std::string BlockMetaPath(BlockID id) const;
    std::string BlockTmpPath(BlockID id) const;

    bool WriteMeta(BlockID id, const BlockMeta& meta);
    bool ReadMeta(BlockID id, BlockMeta* meta) const;

    static uint32_t ComputeCRC32(const std::string& data);
};

}  // namespace mini_storage
