/*
 * benchmark.cpp — Week 4 性能基线测试
 *
 * 测试项目：
 *   1. 顺序写入 QPS（Sequential Write）
 *   2. 随机写入 QPS（Random Write）
 *   3. 顺序读取 QPS —— 无缓存冷读（Cold Read）
 *   4. 顺序读取 QPS —— 有缓存热读（Hot Read，验证 LRU Cache 效果）
 *   5. 随机读取 QPS
 *   6. 混合读写 QPS（70% 读 / 30% 写）
 *   7. 缓存命中率统计
 */

#include "src_db.h"
#include <iostream>
#include <iomanip>
#include <chrono>
#include <vector>
#include <string>
#include <random>
#include <filesystem>
#include <algorithm>

namespace fs = std::filesystem;
using Clock = std::chrono::steady_clock;
using Ms = std::chrono::milliseconds;

// ============================================================
// 辅助工具
// ============================================================

// 格式化打印一行结果
void PrintResult(const std::string& name, int count, double elapsed_ms) {
    double qps = count / (elapsed_ms / 1000.0);
    std::cout << std::left << std::setw(35) << name
        << std::right << std::setw(10) << count << " ops  "
        << std::setw(10) << std::fixed << std::setprecision(2)
        << elapsed_ms << " ms  "
        << std::setw(12) << std::fixed << std::setprecision(0)
        << qps << " QPS" << std::endl;
}

void PrintSeparator() {
    std::cout << std::string(75, '-') << std::endl;
}

// 生成固定长度的字符串
std::string MakeKey(int i, int key_len = 16) {
    std::string k = "key_" + std::to_string(i);
    while ((int)k.size() < key_len) k = "0" + k;
    return k;
}

std::string MakeValue(int i, int val_len = 64) {
    std::string v = "value_" + std::to_string(i);
    while ((int)v.size() < val_len) v += "x";
    return v.substr(0, val_len);
}

static void CleanDir(const std::string& path) {
    if (fs::exists(path))
        for (const auto& e : fs::directory_iterator(path))
            fs::remove_all(e.path());
    else
        fs::create_directories(path);
}

// ============================================================
// Benchmark 1：顺序写入
// ============================================================
void Bench_SequentialWrite(mini_storage::Options& opts, int n) {
    CleanDir(opts.db_path);
    mini_storage::DB db(opts);

    auto t0 = Clock::now();
    for (int i = 0; i < n; i++)
        db.Put(MakeKey(i), MakeValue(i));
    db.FlushMemTable(); // 确保全部落盘
    auto t1 = Clock::now();

    double ms = std::chrono::duration_cast<Ms>(t1 - t0).count();
    PrintResult("顺序写入 (Sequential Write)", n, ms);
}

// ============================================================
// Benchmark 2：随机写入
// ============================================================
void Bench_RandomWrite(mini_storage::Options& opts, int n) {
    CleanDir(opts.db_path);
    mini_storage::DB db(opts);

    std::mt19937 rng(42);
    std::uniform_int_distribution<int> dist(0, n * 10);

    auto t0 = Clock::now();
    for (int i = 0; i < n; i++)
        db.Put(MakeKey(dist(rng)), MakeValue(i));
    db.FlushMemTable();
    auto t1 = Clock::now();

    double ms = std::chrono::duration_cast<Ms>(t1 - t0).count();
    PrintResult("随机写入 (Random Write)", n, ms);
}

// ============================================================
// Benchmark 3：冷读（无缓存命中，每次都读磁盘）
// ============================================================
void Bench_ColdRead(mini_storage::Options& opts, int n) {
    // 先写入数据并刷盘
    CleanDir(opts.db_path);
    {
        mini_storage::DB db(opts);
        for (int i = 0; i < n; i++)
            db.Put(MakeKey(i), MakeValue(i));
        db.FlushMemTable();
    }

    // 重新打开 DB（Cache 是空的）
    mini_storage::DB db(opts);

    std::string val;
    auto t0 = Clock::now();
    for (int i = 0; i < n; i++)
        db.Get(MakeKey(i), &val);
    auto t1 = Clock::now();

    double ms = std::chrono::duration_cast<Ms>(t1 - t0).count();
    PrintResult("冷读取 - 无缓存 (Cold Read)", n, ms);
}

// ============================================================
// Benchmark 4：热读（第二次读，全部命中 LRU Cache）
// ============================================================
void Bench_HotRead(mini_storage::Options& opts, int n) {
    CleanDir(opts.db_path);
    mini_storage::DB db(opts);
    for (int i = 0; i < n; i++)
        db.Put(MakeKey(i), MakeValue(i));
    db.FlushMemTable();

    std::string val;
    // 第一轮：填充 Cache
    for (int i = 0; i < n; i++) db.Get(MakeKey(i), &val);

    // 重置缓存统计（只统计第二轮）
    db.GetCache()->Clear();
    // 再次读入，填满 Cache
    for (int i = 0; i < n; i++) db.Get(MakeKey(i), &val);
    db.GetCache()->Clear(); // 清一次，让下面的统计干净

    // 预热：先读一遍让 cache 填满
    for (int i = 0; i < n; i++) db.Get(MakeKey(i), &val);

    // 第二轮：计时（此时 Cache 已热，全部命中）
    auto t0 = Clock::now();
    for (int i = 0; i < n; i++)
        db.Get(MakeKey(i), &val);
    auto t1 = Clock::now();

    double ms = std::chrono::duration_cast<Ms>(t1 - t0).count();
    PrintResult("热读取 - 全缓存命中 (Hot Read)", n, ms);

    // 打印缓存命中率
    auto* cache = db.GetCache();
    std::cout << "  └─ Cache 命中率: "
        << std::fixed << std::setprecision(1)
        << cache->HitRate() << "%"
        << "  (命中 " << cache->Hits()
        << " / 未命中 " << cache->Misses() << ")" << std::endl;
}

// ============================================================
// Benchmark 5：随机读取
// ============================================================
void Bench_RandomRead(mini_storage::Options& opts, int n) {
    CleanDir(opts.db_path);
    mini_storage::DB db(opts);
    for (int i = 0; i < n; i++)
        db.Put(MakeKey(i), MakeValue(i));
    db.FlushMemTable();

    std::mt19937 rng(42);
    std::uniform_int_distribution<int> dist(0, n - 1);
    std::vector<std::string> keys;
    keys.reserve(n);
    for (int i = 0; i < n; i++) keys.push_back(MakeKey(dist(rng)));

    std::string val;
    auto t0 = Clock::now();
    for (const auto& k : keys) db.Get(k, &val);
    auto t1 = Clock::now();

    double ms = std::chrono::duration_cast<Ms>(t1 - t0).count();
    PrintResult("随机读取 (Random Read)", n, ms);
}

// ============================================================
// Benchmark 6：混合读写（70% 读 / 30% 写）
// ============================================================
void Bench_MixedReadWrite(mini_storage::Options& opts, int n) {
    CleanDir(opts.db_path);
    mini_storage::DB db(opts);

    // 预写一批数据
    for (int i = 0; i < n / 2; i++)
        db.Put(MakeKey(i), MakeValue(i));
    db.FlushMemTable();

    std::mt19937 rng(42);
    std::uniform_int_distribution<int> op_dist(0, 9);   // 0-2=写, 3-9=读
    std::uniform_int_distribution<int> key_dist(0, n - 1);

    std::string val;
    auto t0 = Clock::now();
    for (int i = 0; i < n; i++) {
        int key_idx = key_dist(rng);
        if (op_dist(rng) < 3) {
            db.Put(MakeKey(key_idx), MakeValue(i)); // 30% 写
        }
        else {
            db.Get(MakeKey(key_idx), &val);          // 70% 读
        }
    }
    auto t1 = Clock::now();

    double ms = std::chrono::duration_cast<Ms>(t1 - t0).count();
    PrintResult("混合读写 70R/30W (Mixed RW)", n, ms);
}

// ============================================================
// Benchmark 7：Compaction 耗时
// ============================================================
void Bench_Compaction(mini_storage::Options& opts, int n) {
    CleanDir(opts.db_path);
    // 关闭自动 Compaction，手动控制
    mini_storage::Options tmp = opts;
    tmp.max_sstable_count = 9999;
    mini_storage::DB db(tmp);

    // 写入多批数据，生成多个 SSTable
    int batch_size = n / 4;
    for (int batch = 0; batch < 4; batch++) {
        for (int i = 0; i < batch_size; i++)
            db.Put(MakeKey(batch * batch_size + i), MakeValue(i));
        db.FlushMemTable();
    }

    auto t0 = Clock::now();
    db.Compaction();
    auto t1 = Clock::now();

    double ms = std::chrono::duration_cast<Ms>(t1 - t0).count();
    PrintResult("Compaction (4 SSTable -> 1)", n, ms);
}

// ============================================================
// main
// ============================================================
int benchmark_main() {
    mini_storage::Options opts;
    opts.db_path = "./bench_db";
    opts.write_buffer_size = 64 * 1024 * 1024; // 64MB，避免测试期间自动刷盘干扰
    opts.max_sstable_count = 9999;              // 关闭自动 Compaction
    opts.block_cache_size = 256;               // 256 个 Block 的缓存

    const int N = 10000; // 每个测试的操作次数

    std::cout << "\n";
    std::cout << "╔══════════════════════════════════════════════════════════════════════════╗\n";
    std::cout << "║          Mini Storage Engine — Week 4 Performance Benchmark             ║\n";
    std::cout << "║                    操作次数 N = " << N << " / Key=16B / Value=64B              ║\n";
    std::cout << "╚══════════════════════════════════════════════════════════════════════════╝\n\n";

    PrintSeparator();
    std::cout << std::left << std::setw(35) << "测试项目"
        << std::right << std::setw(10) << "操作数"
        << std::setw(15) << "耗时"
        << std::setw(15) << "QPS" << std::endl;
    PrintSeparator();

    Bench_SequentialWrite(opts, N);
    Bench_RandomWrite(opts, N);
    PrintSeparator();
    Bench_ColdRead(opts, N);
    Bench_HotRead(opts, N);
    Bench_RandomRead(opts, N);
    PrintSeparator();
    Bench_MixedReadWrite(opts, N);
    PrintSeparator();
    Bench_Compaction(opts, N);
    PrintSeparator();

    std::cout << "\n📊 说明：\n"
        << "  冷读 vs 热读的 QPS 差距体现了 LRU Cache 的加速效果\n"
        << "  热读命中率越接近 100% 说明 Cache 越有效\n"
        << "  写入 QPS 受限于 WAL fsync，这是正常的持久化开销\n\n";

    // 清理测试目录
    fs::remove_all(opts.db_path);
    return 0;
}