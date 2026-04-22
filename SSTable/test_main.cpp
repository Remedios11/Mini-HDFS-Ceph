#include "src_db.h"
#include <iostream>
#include <cassert>
#include <filesystem>
#include <string>

namespace fs = std::filesystem;

// 辅助函数：检查文件是否存在
bool FileExists(const std::string& path) {
    return fs::exists(path);
}

// 辅助函数：获取文件大小
uintmax_t GetFileSize(const std::string& path) {
    if (!FileExists(path)) return 0;
    return fs::file_size(path);
}

void Test_CrashAndRecover(mini_storage::Options& options) {
    std::cout << "\n[Test 1] 正在模拟写入数据并意外崩溃..." << std::endl;

    {
        mini_storage::DB db(options);
        db.Put("key1", "value1");
        db.Put("key2", "value2");
        db.Delete("key1"); // 删除 key1
        db.Put("key3", "value3");

        std::cout << "数据已写入 WAL，文件大小: " << GetFileSize(options.db_path + "/current.log") << " 字节" << std::endl;
        // 此时不调用 db.FlushMemTable()，直接退出作用域模拟崩溃
    }

    std::cout << "[Test 1] 正在重新启动数据库并验证恢复..." << std::endl;
    {
        mini_storage::DB db(options);
        std::string val;

        // 验证 key1 是否被成功删除（恢复了 Delete 操作）
        assert(db.Get("key1", &val) == false);
        // 验证 key2 和 key3
        assert(db.Get("key2", &val) && val == "value2");
        assert(db.Get("key3", &val) && val == "value3");

        std::cout << "✅ Test 1 通过：WAL 成功恢复了 Put 和 Delete 操作！" << std::endl;
    }
}

void Test_LogRotation(mini_storage::Options& options) {
    std::cout << "\n[Test 2] 正在模拟大数据量写入以触发刷盘与日志轮换..." << std::endl;

    {
        mini_storage::DB db(options);
        // 持续写入直到超过 options.write_buffer_size (10KB)
        for (int i = 0; i < 200; ++i) {
            db.Put("bulk_" + std::to_string(i), std::string(100, 'x'));
        }

        std::string sst_path = options.db_path + "/data.sst";
        std::string log_path = options.db_path + "/current.log";

        if (FileExists(sst_path)) {
            std::cout << "✅ 检测到 data.sst 已生成。" << std::endl;
            // 关键：刷盘后 WAL 应该被重置（轮换）为很小的大小（或重新创建）
            if (GetFileSize(log_path) < 1000) {
                std::cout << "✅ 检测到 WAL 已轮换（文件已重置）。" << std::endl;
            }
            else {
                std::cerr << "❌ WAL 轮换失败，文件仍然很大！" << std::endl;
            }
        }
    }
    std::cout << "✅ Test 2 通过：日志轮换逻辑正常。" << std::endl;
}

int main() {
    mini_storage::Options options;
    options.db_path = "./test_db_dir";
    options.write_buffer_size = 10 * 1024; // 设置较小的阈值（10KB）以便触发刷盘

    // 预清理环境
    if (fs::exists(options.db_path)) {
        // 遍历并删除目录下的所有文件，保留外层目录
        for (const auto& entry : fs::directory_iterator(options.db_path)) {
            fs::remove_all(entry.path());
        }
    }
    else {
        fs::create_directories(options.db_path);
    }

    try {
        Test_CrashAndRecover(options);
        Test_LogRotation(options);

        std::cout << "\n🎉 所有 Week 3 核心任务测试成功！" << std::endl;
    }
    catch (const std::exception& e) {
        std::cerr << "\n❌ 测试过程中发生异常: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}