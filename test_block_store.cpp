// test/block_store_test.cpp
#include "datanode_block_store.h"
#include <cassert>
#include <iostream>
#include <cstdlib>

int main() {
    // Use a temp directory, clean up first
    system("rm -rf /tmp/dn_test_bs");
    mini_storage::BlockStore store("/tmp/dn_test_bs");

    // Test 1: Write and read (whole block)
    std::string data(1024 * 1024, 'A');  // 1MB
    assert(store.WriteBlock(1, data));
    assert(store.HasBlock(1));

    std::string read_data;
    assert(store.ReadBlock(1, 0, 0, &read_data));
    assert(read_data == data);
    std::cout << "✓ Test 1: WriteBlock / ReadBlock (full)\n";

    // Test 2: Partial read
    std::string partial;
    assert(store.ReadBlock(1, 100, 200, &partial));
    assert(partial.size() == 200);
    assert(partial == data.substr(100, 200));
    std::cout << "✓ Test 2: Partial read\n";

    // Test 3: BlockMeta
    auto meta = store.GetBlockMeta(1);
    assert(meta.has_value());
    assert(meta->size == (int64_t)(1024 * 1024));
    assert(meta->crc32 != 0);
    std::cout << "✓ Test 3: BlockMeta, CRC32=" << meta->crc32 << "\n";

    // Test 4: ListAllBlocks
    assert(store.WriteBlock(2, std::string(512, 'B')));
    assert(store.WriteBlock(3, std::string(256, 'C')));
    auto blocks = store.ListAllBlocks();
    assert(blocks.size() == 3);
    std::cout << "✓ Test 4: ListAllBlocks = " << blocks.size() << " blocks\n";

    // Test 5: Free space
    int64_t free = store.GetFreeSpace();
    assert(free > 0);
    std::cout << "✓ Test 5: FreeSpace = " << free / 1024 / 1024 << " MB\n";

    // Test 6: Delete
    assert(store.DeleteBlock(1));
    assert(!store.HasBlock(1));
    assert(!store.GetBlockMeta(1).has_value());
    std::cout << "✓ Test 6: DeleteBlock\n";

    // Test 7: Overwrite
    std::string data2(512, 'Z');
    assert(store.WriteBlock(2, data2));
    std::string read2;
    assert(store.ReadBlock(2, 0, 0, &read2));
    assert(read2 == data2);
    std::cout << "✓ Test 7: Overwrite block\n";

    std::cout << "\n✅ BlockStore all tests passed!\n";
    return 0;
}
