#include "src_db.h"
#include "Lrucache.h"
#include <iostream>
#include <iomanip>
#include <cassert>
#include <filesystem>
#include <string>

namespace fs = std::filesystem;

bool FileExists(const std::string& path) { return fs::exists(path); }
uintmax_t GetFileSize(const std::string& path) {
    if (!FileExists(path)) return 0;
    return fs::file_size(path);
}
int CountSSTFiles(const std::string& dir) {
    if (!fs::exists(dir)) return 0;
    int cnt = 0;
    for (const auto& e : fs::directory_iterator(dir))
        if (e.path().extension() == ".sst") cnt++;
    return cnt;
}
static void CleanDir(const std::string& path) {
    if (fs::exists(path))
        for (const auto& e : fs::directory_iterator(path))
            fs::remove_all(e.path());
    else
        fs::create_directories(path);
}

// ================================================================
// Test 1：WAL 崩溃恢复
// ================================================================
void Test_CrashAndRecover(mini_storage::Options& options) {
    std::cout << "\n[Test 1] 模拟写入并崩溃..." << std::endl;
    {
        mini_storage::DB db(options);
        db.Put("key1", "value1"); db.Put("key2", "value2");
        db.Delete("key1"); db.Put("key3", "value3");
    }
    {
        mini_storage::DB db(options); std::string val;
        assert(db.Get("key1", &val) == false);
        assert(db.Get("key2", &val) && val == "value2");
        assert(db.Get("key3", &val) && val == "value3");
    }
    std::cout << "✅ Test 1 通过：WAL 崩溃恢复正确！" << std::endl;
}

// ================================================================
// Test 2：日志轮换
// ================================================================
void Test_LogRotation(mini_storage::Options& options) {
    std::cout << "\n[Test 2] 日志轮换..." << std::endl;
    {
        mini_storage::DB db(options);
        for (int i = 0; i < 200; i++) db.Put("bulk_" + std::to_string(i), std::string(100, 'x'));
        db.FlushMemTable();
        assert(CountSSTFiles(options.db_path) >= 1);
        assert(GetFileSize(options.db_path + "/current.log") < 1000);
    }
    std::cout << "✅ Test 2 通过：日志轮换正常！" << std::endl;
}

// ================================================================
// Test 3：SSTable 编号化
// ================================================================
void Test_SSTableNumbering(mini_storage::Options& options) {
    std::cout << "\n[Test 3] SSTable 编号化..." << std::endl;
    {
        mini_storage::DB db(options);
        for (int i = 0; i < 5; i++) db.Put("b1_" + std::to_string(i), "v" + std::to_string(i));
        db.FlushMemTable();
        for (int i = 0; i < 5; i++) db.Put("b2_" + std::to_string(i), "v" + std::to_string(i));
        db.FlushMemTable();
        assert(CountSSTFiles(options.db_path) >= 2);
    }
    std::cout << "✅ Test 3 通过：SSTable 编号化正常！" << std::endl;
}

// ================================================================
// Test 4：刷盘后从 SSTable 读取
// ================================================================
void Test_GetFromSSTable(mini_storage::Options& options) {
    std::cout << "\n[Test 4] 刷盘后从 SSTable 读取..." << std::endl;
    {
        mini_storage::DB db(options);
        db.Put("pk1", "pv1"); db.Put("pk2", "pv2"); db.Put("pk3", "pv3");
        db.FlushMemTable();
        std::string val;
        assert(db.Get("pk1", &val) && val == "pv1");
        assert(db.Get("pk2", &val) && val == "pv2");
        assert(db.Get("pk3", &val) && val == "pv3");
        assert(db.Get("no", &val) == false);
    }
    std::cout << "✅ Test 4 通过：SSTable 读取正确！" << std::endl;
}

// ================================================================
// Test 5：跨 SSTable 的 Delete 语义
// ================================================================
void Test_DeleteAcrossSSTables(mini_storage::Options& options) {
    std::cout << "\n[Test 5] 跨 SSTable Delete 语义..." << std::endl;
    {
        mini_storage::DB db(options);
        db.Put("del_key", "orig"); db.Put("keep_key", "keep");
        db.FlushMemTable();
        db.Delete("del_key"); db.FlushMemTable();
        std::string val;
        assert(db.Get("del_key", &val) == false);
        assert(db.Get("keep_key", &val) && val == "keep");
    }
    std::cout << "✅ Test 5 通过：跨 SSTable Delete 正确！" << std::endl;
}

// ================================================================
// Test 6：Compaction 正确性
// ================================================================
void Test_Compaction(mini_storage::Options& options) {
    std::cout << "\n[Test 6] Compaction 正确性..." << std::endl;
    {
        mini_storage::DB db(options);
        db.Put("dup", "v1"); db.Put("del", "will_del"); db.Put("keep", "keep");
        db.FlushMemTable();
        db.Put("dup", "v2"); db.Delete("del"); db.FlushMemTable();
        db.Put("dup", "v3_final"); db.Put("new", "new_val"); db.FlushMemTable();
        db.Compaction();
        assert(CountSSTFiles(options.db_path) == 1);
        std::string val;
        assert(db.Get("dup", &val) && val == "v3_final");
        assert(db.Get("del", &val) == false);
        assert(db.Get("keep", &val) && val == "keep");
        assert(db.Get("new", &val) && val == "new_val");
    }
    std::cout << "✅ Test 6 通过：Compaction 正确！" << std::endl;
}

// ================================================================
// Test 7：重启后 SSTable 自动恢复
// ================================================================
void Test_RestartWithSSTables(mini_storage::Options& options) {
    std::cout << "\n[Test 7] 重启后 SSTable 自动恢复..." << std::endl;
    {
        mini_storage::DB db(options);
        db.Put("rk1", "rv1"); db.Put("rk2", "rv2"); db.FlushMemTable();
    }
    {
        mini_storage::DB db(options); std::string val;
        assert(db.Get("rk1", &val) && val == "rv1");
        assert(db.Get("rk2", &val) && val == "rv2");
    }
    std::cout << "✅ Test 7 通过：重启 SSTable 自动加载正确！" << std::endl;
}

// ================================================================
// Test 8：自动触发 Compaction
// ================================================================
void Test_AutoCompaction(mini_storage::Options& options) {
    std::cout << "\n[Test 8] 自动触发 Compaction..." << std::endl;
    {
        mini_storage::DB db(options);
        for (int b = 0; b < options.max_sstable_count; b++) {
            for (int i = 0; i < 3; i++)
                db.Put("ak_" + std::to_string(b * 10 + i), "av_" + std::to_string(b * 10 + i));
            db.FlushMemTable();
        }
        assert(CountSSTFiles(options.db_path) <= 1);
        std::string val;
        assert(db.Get("ak_0", &val) && val == "av_0");
    }
    std::cout << "✅ Test 8 通过：自动 Compaction 正常！" << std::endl;
}

// ================================================================
// Test 9：LRU Cache 基础功能（插入 / 命中 / 淘汰）
// ================================================================
void Test_LRUCache_Basic() {
    std::cout << "\n[Test 9] LRU Cache 基础功能..." << std::endl;

    mini_storage::LRUCache cache(3); // 容量 3
    mini_storage::BlockData b1 = { {"k1","v1"},{"k2","v2"} };
    mini_storage::BlockData b2 = { {"k3","v3"} };
    mini_storage::BlockData b3 = { {"k4","v4"} };
    mini_storage::BlockData b4 = { {"k5","v5"} };

    cache.Insert("f:0", b1);
    cache.Insert("f:100", b2);
    cache.Insert("f:200", b3);
    assert(cache.Size() == 3);

    mini_storage::BlockData out;
    assert(cache.Get("f:0", &out) == true && out[0].first == "k1");
    assert(cache.Get("f:999", &out) == false);

    // 访问 f:0 使其成为最近使用，之后插入 f:300 应淘汰 f:100
    cache.Get("f:0", &out);
    cache.Insert("f:300", b4);
    assert(cache.Size() == 3);
    assert(cache.Get("f:100", &out) == false); // 被淘汰
    assert(cache.Get("f:0", &out) == true);
    assert(cache.Get("f:200", &out) == true);
    assert(cache.Get("f:300", &out) == true);

    std::cout << "  命中率: " << std::fixed << std::setprecision(1)
        << cache.HitRate() << "%" << std::endl;
    std::cout << "✅ Test 9 通过：LRU Cache 插入/命中/淘汰正确！" << std::endl;
}

// ================================================================
// Test 10：LRU Cache 集成 DB，验证热读命中率
// ================================================================
void Test_LRUCache_Integrated(mini_storage::Options& options) {
    std::cout << "\n[Test 10] LRU Cache 集成 DB，验证命中率..." << std::endl;
    CleanDir(options.db_path);
    mini_storage::DB db(options);

    for (int i = 0; i < 100; i++)
        db.Put("ck_" + std::to_string(i), "cv_" + std::to_string(i));
    db.FlushMemTable();

    std::string val;
    // 第一轮：冷读，全部 Miss
    for (int i = 0; i < 100; i++) db.Get("ck_" + std::to_string(i), &val);
    uint64_t cold_miss = db.GetCache()->Misses();

    // 第二轮：热读，应大量命中
    for (int i = 0; i < 100; i++) db.Get("ck_" + std::to_string(i), &val);
    uint64_t hot_hits = db.GetCache()->Hits();
    double   rate = db.GetCache()->HitRate();

    std::cout << "  冷读 Miss=" << cold_miss
        << "  热读 Hit=" << hot_hits
        << "  命中率=" << std::fixed << std::setprecision(1)
        << rate << "%" << std::endl;

    assert(hot_hits > 0);
    assert(rate > 40.0); // 第二轮全命中，两轮合计 > 50%
    std::cout << "✅ Test 10 通过：LRU Cache 成功加速读取！" << std::endl;
}

// ================================================================
// main
// ================================================================
int main() {
    mini_storage::Options options;
    options.db_path = "./test_db_dir";
    options.write_buffer_size = 10 * 1024; // 10KB
    options.max_sstable_count = 4;
    options.block_cache_size = 64;

    auto reset = [&]() { CleanDir(options.db_path); };

    try {
        reset(); Test_CrashAndRecover(options);
        reset(); Test_LogRotation(options);
        reset(); Test_SSTableNumbering(options);
        reset(); Test_GetFromSSTable(options);
        reset(); Test_DeleteAcrossSSTables(options);
        reset(); Test_Compaction(options);
        reset(); Test_RestartWithSSTables(options);
        reset(); Test_AutoCompaction(options);
        // Week 4 收尾
        Test_LRUCache_Basic();
        reset(); Test_LRUCache_Integrated(options);

        std::cout << "\n🎉 所有 Week 4（含收尾）测试全部通过！\n";
    }
    catch (const std::exception& e) {
        std::cerr << "\n❌ 异常: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}