// test/namenode_test.cpp
// Week 6 integration test: NameNode RPC flow
#include "namenode_namenode_server.h"
#include "proto/namenode.pb.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <thread>
#include <chrono>
#include <cassert>
#include <iostream>
#include <cstdlib>

// ===== Wire-protocol helpers =====

static bool SendMsg(int fd, const std::string& data) {
    uint32_t len = htonl((uint32_t)data.size());
    if (write(fd, &len, 4) != 4) return false;
    return write(fd, data.data(), (ssize_t)data.size()) == (ssize_t)data.size();
}

static bool RecvMsg(int fd, std::string* data) {
    uint32_t len;
    if (read(fd, &len, 4) != 4) return false;
    len = ntohl(len);
    data->resize(len);
    return read(fd, &(*data)[0], len) == (ssize_t)len;
}

static mini_storage::NameNodeResponse SendRequest(
    int fd, const mini_storage::NameNodeRequest& req) {
    std::string bytes;
    req.SerializeToString(&bytes);
    assert(SendMsg(fd, bytes));
    std::string resp_bytes;
    assert(RecvMsg(fd, &resp_bytes));
    mini_storage::NameNodeResponse resp;
    assert(resp.ParseFromString(resp_bytes));
    return resp;
}

// ===== Test =====

int main() {
    std::cout << "=== Week 6: NameNode Integration Test ===\n";
    system("rm -rf /tmp/nn_test_w6");

    // 1. Start NameNode in background thread
    mini_storage::NameNodeServer nn("/tmp/nn_test_w6", "127.0.0.1", 19090, 2);
    assert(nn.Start());
    std::thread nn_thread([&nn] { nn.Run(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 2. Connect client
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(19090);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    assert(connect(fd, (sockaddr*)&addr, sizeof(addr)) == 0);

    // 3. Register 3 DataNodes
    for (int i = 1; i <= 3; i++) {
        mini_storage::NameNodeRequest req;
        req.set_op(mini_storage::NameNodeRequest::REGISTER_DN);
        req.set_request_id(i);
        auto* rdn = req.mutable_register_dn();
        rdn->set_datanode_id("127.0.0.1:900" + std::to_string(i));
        rdn->set_host("127.0.0.1");
        rdn->set_port(9000 + i);
        rdn->set_free_space(10LL * 1024 * 1024 * 1024);  // 10 GB

        auto resp = SendRequest(fd, req);
        assert(resp.success());
        std::cout << "✓ Registered DataNode " << i << "\n";
    }

    // 4. Create file
    {
        mini_storage::NameNodeRequest req;
        req.set_op(mini_storage::NameNodeRequest::CREATE_FILE);
        req.set_request_id(10);
        req.mutable_create_file()->set_path("/user/alice/data.txt");
        auto resp = SendRequest(fd, req);
        assert(resp.success());
        std::cout << "✓ CreateFile /user/alice/data.txt\n";
    }

    // 5. Duplicate create → must fail
    {
        mini_storage::NameNodeRequest req;
        req.set_op(mini_storage::NameNodeRequest::CREATE_FILE);
        req.set_request_id(11);
        req.mutable_create_file()->set_path("/user/alice/data.txt");
        auto resp = SendRequest(fd, req);
        assert(!resp.success());
        std::cout << "✓ Duplicate CreateFile correctly rejected\n";
    }

    // 6. AllocateBlock
    uint64_t block_id = 0;
    {
        mini_storage::NameNodeRequest req;
        req.set_op(mini_storage::NameNodeRequest::ALLOCATE_BLOCK);
        req.set_request_id(20);
        req.mutable_allocate_block()->set_file_path("/user/alice/data.txt");
        auto resp = SendRequest(fd, req);
        assert(resp.success());
        assert(resp.has_allocate_block());
        block_id = resp.allocate_block().block_id();
        int dn_count = resp.allocate_block().datanode_addresses_size();
        assert(dn_count >= 1);
        std::cout << "✓ AllocateBlock id=" << block_id
                  << " targets=" << dn_count << "\n";
        for (int i = 0; i < dn_count; i++)
            std::cout << "    → " << resp.allocate_block().datanode_addresses(i) << "\n";
    }

    // 7. GetFileBlocks
    {
        mini_storage::NameNodeRequest req;
        req.set_op(mini_storage::NameNodeRequest::GET_FILE_BLOCKS);
        req.set_request_id(30);
        req.mutable_get_file_blocks()->set_path("/user/alice/data.txt");
        auto resp = SendRequest(fd, req);
        assert(resp.success());
        assert(resp.has_get_file_blocks());
        assert(resp.get_file_blocks().blocks_size() == 1);
        std::cout << "✓ GetFileBlocks: 1 block\n";
    }

    // 8. Heartbeat
    {
        mini_storage::NameNodeRequest req;
        req.set_op(mini_storage::NameNodeRequest::HEARTBEAT);
        req.set_request_id(40);
        req.mutable_heartbeat()->set_datanode_id("127.0.0.1:9001");
        req.mutable_heartbeat()->set_free_space(9LL * 1024 * 1024 * 1024);
        req.mutable_heartbeat()->set_block_count(1);
        auto resp = SendRequest(fd, req);
        assert(resp.success());
        std::cout << "✓ Heartbeat accepted\n";
    }

    // 9. BlockReport
    {
        mini_storage::NameNodeRequest req;
        req.set_op(mini_storage::NameNodeRequest::BLOCK_REPORT);
        req.set_request_id(50);
        auto* br = req.mutable_block_report();
        br->set_datanode_id("127.0.0.1:9001");
        auto* bm = br->add_blocks();
        bm->set_block_id(block_id);
        bm->set_size(1024);
        bm->set_crc32(0x12345678);
        auto resp = SendRequest(fd, req);
        assert(resp.success());
        std::cout << "✓ BlockReport accepted\n";
    }

    // 10. ListFiles
    {
        mini_storage::NameNodeRequest req;
        req.set_op(mini_storage::NameNodeRequest::LIST_FILES);
        req.set_request_id(60);
        req.mutable_list_files()->set_dir("/user/alice");
        auto resp = SendRequest(fd, req);
        assert(resp.success());
        assert(resp.has_list_files());
        assert(resp.list_files().files_size() >= 1);
        std::cout << "✓ ListFiles: " << resp.list_files().files_size() << " file(s)\n";
    }

    // 11. Delete file
    {
        mini_storage::NameNodeRequest req;
        req.set_op(mini_storage::NameNodeRequest::DELETE_FILE);
        req.set_request_id(70);
        req.mutable_delete_file()->set_path("/user/alice/data.txt");
        auto resp = SendRequest(fd, req);
        assert(resp.success());
        std::cout << "✓ DeleteFile\n";
    }

    // 12. GetFileBlocks after delete → fail
    {
        mini_storage::NameNodeRequest req;
        req.set_op(mini_storage::NameNodeRequest::GET_FILE_BLOCKS);
        req.set_request_id(80);
        req.mutable_get_file_blocks()->set_path("/user/alice/data.txt");
        auto resp = SendRequest(fd, req);
        assert(!resp.success());
        std::cout << "✓ GetFileBlocks after delete correctly fails\n";
    }

    std::cout << "\n✅ Week 6 NameNode integration test PASSED!\n";
    close(fd);
    nn.Stop();
    nn_thread.join();
    return 0;
}
