// test/end_to_end_test.cpp
// Week 7 end-to-end: NameNode + 3 DataNodes + StorageClient
#include "namenode_namenode_server.h"
#include "datanode_datanode_server.h"
#include "client_storage_client.h"
#include <thread>
#include <chrono>
#include <cassert>
#include <iostream>
#include <cstdlib>
#include <string>

int main() {
    std::cout << "=== Week 7: End-to-End Integration Test ===\n";
    system("rm -rf /tmp/e2e_nn /tmp/e2e_dn1 /tmp/e2e_dn2 /tmp/e2e_dn3");

    // 1. Start NameNode
    mini_storage::NameNodeServer namenode(
        "/tmp/e2e_nn", "127.0.0.1", 19190, 4);
    assert(namenode.Start());
    std::thread nn_thread([&namenode] { namenode.Run(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    std::cout << "✓ NameNode started on :19190\n";

    // 2. Start 3 DataNodes
    auto dn1 = std::make_unique<mini_storage::DataNodeServer>(
        "/tmp/e2e_dn1", "127.0.0.1", 19191, "127.0.0.1", 19190);
    auto dn2 = std::make_unique<mini_storage::DataNodeServer>(
        "/tmp/e2e_dn2", "127.0.0.1", 19192, "127.0.0.1", 19190);
    auto dn3 = std::make_unique<mini_storage::DataNodeServer>(
        "/tmp/e2e_dn3", "127.0.0.1", 19193, "127.0.0.1", 19190);

    assert(dn1->Start());
    assert(dn2->Start());
    assert(dn3->Start());
    // Wait for registration + heartbeat to propagate
    std::this_thread::sleep_for(std::chrono::milliseconds(600));
    std::cout << "✓ 3 DataNodes started and registered\n";

    // 3. Write a small file (< kBlockSize, single block)
    mini_storage::StorageClient client("127.0.0.1", 19190);
    std::string content = "Hello, distributed world! " + std::string(1024, 'X');
    assert(client.PutFile("/test/hello.txt", content));
    std::cout << "✓ PutFile /test/hello.txt (" << content.size() << " bytes)\n";

    // 4. Read back and verify
    std::string read_back;
    assert(client.GetFile("/test/hello.txt", &read_back));
    assert(read_back == content);
    std::cout << "✓ GetFile verified (content matches)\n";

    // 5. Write a larger file (> kBlockSize → 2 blocks)
    std::string big_data(5 * 1024 * 1024, 'Y');  // 5 MB
    assert(client.PutFile("/test/big.dat", big_data));
    std::cout << "✓ PutFile /test/big.dat (5 MB, 2 blocks)\n";

    std::string big_read;
    assert(client.GetFile("/test/big.dat", &big_read));
    assert(big_read == big_data);
    std::cout << "✓ GetFile /test/big.dat verified\n";

    // 6. Write a file with exact block size
    std::string exact_block(4 * 1024 * 1024, 'E');
    assert(client.PutFile("/test/exact.dat", exact_block));
    std::string exact_read;
    assert(client.GetFile("/test/exact.dat", &exact_read));
    assert(exact_read == exact_block);
    std::cout << "✓ PutFile/GetFile exact 4MB block\n";

    // 7. Delete file
    assert(client.DeleteFile("/test/hello.txt"));
    std::string deleted;
    bool got = client.GetFile("/test/hello.txt", &deleted);
    assert(!got);
    std::cout << "✓ DeleteFile verified (file gone)\n";

    // 8. Verify remaining file still readable
    std::string still_ok;
    assert(client.GetFile("/test/big.dat", &still_ok));
    assert(still_ok == big_data);
    std::cout << "✓ Remaining files unaffected by delete\n";

    // 9. Write empty-ish file (1 byte)
    std::string tiny = "Z";
    assert(client.PutFile("/test/tiny.txt", tiny));
    std::string tiny_read;
    assert(client.GetFile("/test/tiny.txt", &tiny_read));
    assert(tiny_read == tiny);
    std::cout << "✓ Single-byte file OK\n";

    std::cout << "\n✅ Week 7 End-to-End integration test PASSED!\n";

    // Cleanup
    dn1->Stop();
    dn2->Stop();
    dn3->Stop();
    namenode.Stop();
    nn_thread.join();
    return 0;
}
