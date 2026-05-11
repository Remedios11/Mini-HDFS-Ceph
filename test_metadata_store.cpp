// test/metadata_store_test.cpp
#include "namenode_metadata_store.h"
#include <cassert>
#include <iostream>

int main() {
    mini_storage::MetadataStore store;

    // 1. Create file
    mini_storage::FileInfo info;
    assert(store.CreateFile("/user/alice/test.txt", &info));
    assert(!store.CreateFile("/user/alice/test.txt", &info));  // duplicate fails
    std::cout << "✓ CreateFile (duplicate rejection)\n";

    // 2. Get file
    auto f = store.GetFile("/user/alice/test.txt");
    assert(f.has_value());
    assert(f->path == "/user/alice/test.txt");
    assert(!store.GetFile("/nonexistent").has_value());
    std::cout << "✓ GetFile\n";

    // 3. Update file (add block)
    f->blocks.push_back(100);
    f->size = 4 * 1024 * 1024;
    assert(store.UpdateFile("/user/alice/test.txt", *f));
    auto f2 = store.GetFile("/user/alice/test.txt");
    assert(f2->blocks.size() == 1 && f2->blocks[0] == 100);
    std::cout << "✓ UpdateFile\n";

    // 4. Block operations
    mini_storage::BlockInfo block;
    block.block_id  = 100;
    block.size      = 4 * 1024 * 1024;
    block.locations = {"127.0.0.1:9001", "127.0.0.1:9002"};
    store.AddBlock(block);

    auto b = store.GetBlock(100);
    assert(b.has_value());
    assert(b->locations.size() == 2);
    std::cout << "✓ AddBlock / GetBlock\n";

    // 5. UpdateBlockLocation
    store.UpdateBlockLocation(100, "127.0.0.1:9003");
    auto locs = store.GetBlockLocations(100);
    assert(locs.size() == 3);
    // Idempotent
    store.UpdateBlockLocation(100, "127.0.0.1:9003");
    assert(store.GetBlockLocations(100).size() == 3);
    std::cout << "✓ UpdateBlockLocation (idempotent)\n";

    // 6. ListFiles
    store.CreateFile("/user/alice/b.txt", nullptr);
    store.CreateFile("/user/bob/c.txt", nullptr);
    auto alice_files = store.ListFiles("/user/alice");
    assert(alice_files.size() == 2);
    std::cout << "✓ ListFiles\n";

    // 7. Delete file
    assert(store.DeleteFile("/user/alice/test.txt"));
    assert(!store.GetFile("/user/alice/test.txt").has_value());
    assert(!store.DeleteFile("/nonexistent"));
    std::cout << "✓ DeleteFile\n";

    // 8. Delete block
    assert(store.DeleteBlock(100));
    assert(!store.GetBlock(100).has_value());
    std::cout << "✓ DeleteBlock\n";

    std::cout << "\n✅ MetadataStore all tests passed!\n";
    return 0;
}
