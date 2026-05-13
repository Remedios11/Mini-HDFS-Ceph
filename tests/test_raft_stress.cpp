// test_raft_stress.cpp
// Stress tests for Raft consensus implementation
//
// Port allocation:
//   Stress 1 (massive log):    raft ports 21001, 21002, 21003
//   Stress 2 (concurrent):     raft ports 21011, 21012, 21013
//   Stress 3 (leader churn):   raft ports 21021, 21022, 21023
//   Stress 4 (partition):      raft ports 21031, 21032, 21033
//   Stress 5 (large payload):  raft ports 21041
//   Stress 6 (persistence):    raft ports 21051
//   Stress 7 (election churn): raft ports 21061, 21062, 21063
//   Stress 8 (HA stress):      client 21071,21081,21091 / raft 21072,21082,21092 / DN 21073,21083,21093

#include "raft/raft_node.h"
#include "raft/raft_rpc.h"
#include "namenode/namenode_ha_server.h"
#include "client/client_storage_client.h"
#include "datanode/datanode_datanode_server.h"
#include "proto/namenode.pb.h"
#include "proto/raft.pb.h"
#include <algorithm>
#include <thread>
#include <chrono>
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <vector>
#include <atomic>
#include <sstream>
#include <iomanip>
#include <mutex>
#include <cmath>

using namespace mini_storage;

static void Sleep(int ms) {
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

static double NowMs() {
    static auto start = std::chrono::steady_clock::now();
    auto now = std::chrono::steady_clock::now();
    return std::chrono::duration<double, std::milli>(now - start).count();
}

// ===================================================================
// LatencyRecorder — thread-safe latency collector with percentile calc
// ===================================================================
class LatencyRecorder {
public:
    void Record(double latency_ms) {
        std::lock_guard<std::mutex> lk(mutex_);
        samples_.push_back(latency_ms);
        sum_ += latency_ms;
    }

    size_t Count() const {
        std::lock_guard<std::mutex> lk(mutex_);
        return samples_.size();
    }

    double Avg() const {
        std::lock_guard<std::mutex> lk(mutex_);
        if (samples_.empty()) return 0.0;
        return sum_ / samples_.size();
    }

    double Min() const {
        std::lock_guard<std::mutex> lk(mutex_);
        if (samples_.empty()) return 0.0;
        return *std::min_element(samples_.begin(), samples_.end());
    }

    double Max() const {
        std::lock_guard<std::mutex> lk(mutex_);
        if (samples_.empty()) return 0.0;
        return *std::max_element(samples_.begin(), samples_.end());
    }

    double P50() const { return Percentile(50.0); }
    double P99() const { return Percentile(99.0); }
    double P999() const { return Percentile(99.9); }

    void Reset() {
        std::lock_guard<std::mutex> lk(mutex_);
        samples_.clear();
        sum_ = 0.0;
    }

private:
    double Percentile(double pct) const {
        std::lock_guard<std::mutex> lk(mutex_);
        if (samples_.empty()) return 0.0;
        auto sorted = samples_;  // copy
        std::sort(sorted.begin(), sorted.end());
        double rank = pct / 100.0 * (sorted.size() - 1);
        size_t lo = (size_t)std::floor(rank);
        size_t hi = (size_t)std::ceil(rank);
        if (lo == hi) return sorted[lo];
        double frac = rank - lo;
        return sorted[lo] * (1.0 - frac) + sorted[hi] * frac;
    }

    mutable std::mutex mutex_;
    std::vector<double> samples_;
    double sum_ = 0.0;
};

// ===================================================================
// MetricsReporter — prints a formatted summary block
// ===================================================================
struct MetricsReporter {
    static void PrintWriteStats(const char* label, const LatencyRecorder& rec,
                                 double elapsed_ms) {
        size_t n = rec.Count();
        if (n == 0) return;
        double qps = n / (elapsed_ms / 1000.0);
        std::cout << "\n  -- " << label << " Write Metrics --\n";
        std::cout << "    请求数:    " << n << "\n";
        std::cout << "    总耗时:    " << std::fixed << std::setprecision(0) << elapsed_ms << " ms\n";
        std::cout << "    写入 QPS:  " << std::fixed << std::setprecision(1) << qps << "\n";
        std::cout << "    P50 延迟:  " << std::fixed << std::setprecision(2) << rec.P50() << " ms\n";
        std::cout << "    P99 延迟:  " << std::fixed << std::setprecision(2) << rec.P99() << " ms\n";
        std::cout << "    P999 延迟: " << std::fixed << std::setprecision(2) << rec.P999() << " ms\n";
        std::cout << "    平均延迟:  " << std::fixed << std::setprecision(2) << rec.Avg() << " ms\n";
        std::cout << "    最小延迟:  " << std::fixed << std::setprecision(2) << rec.Min() << " ms\n";
        std::cout << "    最大延迟:  " << std::fixed << std::setprecision(2) << rec.Max() << " ms\n";
    }

    static void PrintReadStats(const char* label, const LatencyRecorder& rec,
                                double elapsed_ms) {
        size_t n = rec.Count();
        if (n == 0) return;
        double qps = n / (elapsed_ms / 1000.0);
        std::cout << "\n  -- " << label << " Read Metrics --\n";
        std::cout << "    请求数:    " << n << "\n";
        std::cout << "    总耗时:    " << std::fixed << std::setprecision(0) << elapsed_ms << " ms\n";
        std::cout << "    读取 QPS:  " << std::fixed << std::setprecision(1) << qps << "\n";
        std::cout << "    P50 延迟:  " << std::fixed << std::setprecision(2) << rec.P50() << " ms\n";
        std::cout << "    P99 延迟:  " << std::fixed << std::setprecision(2) << rec.P99() << " ms\n";
        std::cout << "    P999 延迟: " << std::fixed << std::setprecision(2) << rec.P999() << " ms\n";
        std::cout << "    平均延迟:  " << std::fixed << std::setprecision(2) << rec.Avg() << " ms\n";
        std::cout << "    最小延迟:  " << std::fixed << std::setprecision(2) << rec.Min() << " ms\n";
        std::cout << "    最大延迟:  " << std::fixed << std::setprecision(2) << rec.Max() << " ms\n";
    }
};

// Helper: create a protobuf command string
static std::string MakeCommand(const std::string& path) {
    NameNodeRequest req;
    req.set_op(NameNodeRequest::CREATE_FILE);
    req.mutable_create_file()->set_path(path);
    std::string cmd;
    req.SerializeToString(&cmd);
    return cmd;
}

// Helper: wire RPC between RaftNodes via direct calls (no network)
static void WireNodes(std::vector<RaftNode*>& nodes,
                      const std::vector<std::string>& peers) {
    for (auto* node : nodes) {
        node->SetSendRPCCallback([&nodes, &peers, node]
            (const std::string& peer_id, const RaftMessage& msg) {
            RaftNode* target = nullptr;
            for (size_t i = 0; i < peers.size(); i++) {
                if (peers[i] == peer_id && i < nodes.size()) {
                    target = nodes[i];
                    break;
                }
            }
            if (!target) return;

            if (msg.type() == RaftMessage::REQUEST_VOTE) {
                auto* resp = target->HandleRequestVote(
                    msg.request_vote().candidate_id(), msg.request_vote());
                if (resp) node->HandleRequestVoteResponse(peer_id, *resp);
            } else if (msg.type() == RaftMessage::APPEND_ENTRIES) {
                auto* resp = target->HandleAppendEntries(
                    msg.append_entries().leader_id(), msg.append_entries());
                if (resp) node->HandleAppendEntriesResponse(peer_id, *resp);
            } else if (msg.type() == RaftMessage::INSTALL_SNAPSHOT) {
                (void)target->HandleInstallSnapshot(
                    msg.install_snapshot().leader_id(), msg.install_snapshot());
            }
        });
    }
}

// ===================================================================
// Stress Test 1: Massive Log Replication (1000 entries)
// ===================================================================
static void StressMassiveLogReplication() {
    std::cout << "\n===== Stress 1: Massive Log Replication (1000 entries) =====\n";
    (void)system("rm -rf /tmp/stress1_n1 /tmp/stress1_n2 /tmp/stress1_n3");

    std::vector<std::string> peers = {
        "127.0.0.1:21001", "127.0.0.1:21002", "127.0.0.1:21003",
    };

    RaftConfig cfg1; cfg1.node_id = peers[0]; cfg1.peers = peers;
    cfg1.data_dir = "/tmp/stress1_n1"; cfg1.election_timeout_ms = 1500;
    RaftConfig cfg2; cfg2.node_id = peers[1]; cfg2.peers = peers;
    cfg2.data_dir = "/tmp/stress1_n2"; cfg2.election_timeout_ms = 5000;
    RaftConfig cfg3; cfg3.node_id = peers[2]; cfg3.peers = peers;
    cfg3.data_dir = "/tmp/stress1_n3"; cfg3.election_timeout_ms = 5000;

    RaftNode n1(cfg1), n2(cfg2), n3(cfg3);
    std::vector<RaftNode*> nodes = {&n1, &n2, &n3};
    WireNodes(nodes, peers);

    std::atomic<int> n1_applied{0}, n2_applied{0}, n3_applied{0};
    n1.SetApplyCallback([&](uint64_t, const std::string&) { n1_applied++; });
    n2.SetApplyCallback([&](uint64_t, const std::string&) { n2_applied++; });
    n3.SetApplyCallback([&](uint64_t, const std::string&) { n3_applied++; });

    n1.Start(); n2.Start(); n3.Start();
    std::cout << "Waiting for leader election...\n";
    Sleep(3000);

    RaftNode* leader = nullptr;
    if (n1.IsLeader()) leader = &n1;
    else if (n2.IsLeader()) leader = &n2;
    else if (n3.IsLeader()) leader = &n3;
    assert(leader != nullptr);
    std::cout << "Leader: " << leader->GetLeaderID() << "\n";

    const int kNumEntries = 1000;
    LatencyRecorder write_lat;
    int failed = 0;

    double start = NowMs();
    for (int i = 0; i < kNumEntries; i++) {
        double t0 = NowMs();
        auto result = leader->Propose(MakeCommand("/stress1/file" + std::to_string(i)), 10000);
        write_lat.Record(NowMs() - t0);
        if (!result.committed) failed++;
        if (i > 0 && i % 100 == 0) {
            std::cout << "  Proposed " << i << "/" << kNumEntries
                      << " (elapsed: " << (NowMs() - start) << "ms)\n";
        }
    }
    double elapsed = NowMs() - start;

    MetricsReporter::PrintWriteStats("Stress1", write_lat, elapsed);
    std::cout << "  提交成功: " << (kNumEntries - failed) << "/" << kNumEntries
              << "  失败: " << failed << "\n";

    Sleep(2000);
    assert(n1.GetLogSize() >= (size_t)(kNumEntries));
    assert(n2.GetLogSize() >= (size_t)(kNumEntries));
    assert(n3.GetLogSize() >= (size_t)(kNumEntries));
    std::cout << "Followers synced: n2=" << n2.GetLogSize()
              << " n3=" << n3.GetLogSize() << "\n";

    n1.Stop(); n2.Stop(); n3.Stop();
    (void)system("rm -rf /tmp/stress1_n1 /tmp/stress1_n2 /tmp/stress1_n3");
    std::cout << "Stress 1 PASSED\n";
}

// ===================================================================
// Stress Test 2: Concurrent Proposals (multiple threads)
// ===================================================================
static void StressConcurrentProposals() {
    std::cout << "\n===== Stress 2: Concurrent Proposals (8 threads x 50 entries) =====\n";
    (void)system("rm -rf /tmp/stress2_n1 /tmp/stress2_n2 /tmp/stress2_n3");

    std::vector<std::string> peers = {
        "127.0.0.1:21011", "127.0.0.1:21012", "127.0.0.1:21013",
    };

    RaftConfig cfg1; cfg1.node_id = peers[0]; cfg1.peers = peers;
    cfg1.data_dir = "/tmp/stress2_n1"; cfg1.election_timeout_ms = 1500;
    RaftConfig cfg2; cfg2.node_id = peers[1]; cfg2.peers = peers;
    cfg2.data_dir = "/tmp/stress2_n2"; cfg2.election_timeout_ms = 5000;
    RaftConfig cfg3; cfg3.node_id = peers[2]; cfg3.peers = peers;
    cfg3.data_dir = "/tmp/stress2_n3"; cfg3.election_timeout_ms = 5000;

    RaftNode n1(cfg1), n2(cfg2), n3(cfg3);
    std::vector<RaftNode*> nodes = {&n1, &n2, &n3};
    WireNodes(nodes, peers);

    std::atomic<int> total_applied{0};
    n1.SetApplyCallback([&](uint64_t, const std::string&) { total_applied++; });
    n2.SetApplyCallback([&](uint64_t, const std::string&) { total_applied++; });
    n3.SetApplyCallback([&](uint64_t, const std::string&) { total_applied++; });

    n1.Start(); n2.Start(); n3.Start();
    Sleep(3000);

    assert(n1.IsLeader());
    std::cout << "Leader: n1\n";

    const int kThreads = 8;
    const int kPerThread = 50;
    std::vector<std::thread> threads;
    std::atomic<int> committed{0};
    std::atomic<int> rejected{0};
    LatencyRecorder write_lat;
    std::mutex lat_mutex;  // for LatencyRecorder (already thread-safe, but let's verify)

    double start = NowMs();
    for (int t = 0; t < kThreads; t++) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < kPerThread; i++) {
                auto path = "/stress2/t" + std::to_string(t) + "_" + std::to_string(i);
                double t0 = NowMs();
                auto result = n1.Propose(MakeCommand(path), 10000);
                write_lat.Record(NowMs() - t0);
                if (result.committed) committed++;
                else rejected++;
            }
        });
    }

    for (auto& th : threads) th.join();
    double elapsed = NowMs() - start;

    int total = kThreads * kPerThread;
    MetricsReporter::PrintWriteStats("Stress2", write_lat, elapsed);
    assert(committed == total);

    Sleep(1000);
    assert(n1.GetLogSize() >= (size_t)total);
    assert(n2.GetLogSize() >= (size_t)total);
    assert(n3.GetLogSize() >= (size_t)total);

    n1.Stop(); n2.Stop(); n3.Stop();
    (void)system("rm -rf /tmp/stress2_n1 /tmp/stress2_n2 /tmp/stress2_n3");
    std::cout << "Stress 2 PASSED\n";
}

// ===================================================================
// Stress Test 3: Leader Churn (repeatedly kill and elect leaders)
// ===================================================================
static void StressLeaderChurn() {
    std::cout << "\n===== Stress 3: Leader Churn (5 cycles) =====\n";
    (void)system("rm -rf /tmp/stress3_n1 /tmp/stress3_n2 /tmp/stress3_n3");

    std::vector<std::string> peers = {
        "127.0.0.1:21021", "127.0.0.1:21022", "127.0.0.1:21023",
    };

    const int kCycles = 5;
    int successful_elections = 0;
    LatencyRecorder election_lat;  // time from start to leader elected
    LatencyRecorder write_lat;

    for (int cycle = 0; cycle < kCycles; cycle++) {
        std::cout << "\n--- Churn cycle " << (cycle + 1) << "/" << kCycles << " ---\n";
        (void)system("rm -rf /tmp/stress3_n1 /tmp/stress3_n2 /tmp/stress3_n3");

        RaftConfig cfg1; cfg1.node_id = peers[0]; cfg1.peers = peers;
        cfg1.data_dir = "/tmp/stress3_n1"; cfg1.election_timeout_ms = 1500;
        RaftConfig cfg2; cfg2.node_id = peers[1]; cfg2.peers = peers;
        cfg2.data_dir = "/tmp/stress3_n2"; cfg2.election_timeout_ms = 2000;
        RaftConfig cfg3; cfg3.node_id = peers[2]; cfg3.peers = peers;
        cfg3.data_dir = "/tmp/stress3_n3"; cfg3.election_timeout_ms = 2500;

        RaftNode n1(cfg1), n2(cfg2), n3(cfg3);
        std::vector<RaftNode*> nodes = {&n1, &n2, &n3};
        WireNodes(nodes, peers);

        double t_elect_start = NowMs();
        n1.Start(); n2.Start(); n3.Start();
        Sleep(4000);

        int leaders = 0;
        if (n1.IsLeader()) leaders++;
        if (n2.IsLeader()) leaders++;
        if (n3.IsLeader()) leaders++;

        if (leaders == 1) {
            successful_elections++;
            election_lat.Record(NowMs() - t_elect_start);

            RaftNode* leader = nullptr;
            if (n1.IsLeader()) leader = &n1;
            else if (n2.IsLeader()) leader = &n2;
            else leader = &n3;

            for (int i = 0; i < 5; i++) {
                double t0 = NowMs();
                auto r = leader->Propose(
                    MakeCommand("/stress3/c" + std::to_string(cycle) + "_" + std::to_string(i)),
                    3000);
                write_lat.Record(NowMs() - t0);
                assert(r.committed);
            }
            std::cout << "  Leader elected (" << leader->GetLeaderID()
                      << "), 5 entries committed\n";
        } else {
            std::cout << "  WARNING: " << leaders << " leaders (split brain?)\n";
        }

        n1.Stop(); n2.Stop(); n3.Stop();
        Sleep(500);
    }

    assert(successful_elections == kCycles);

    std::cout << "\n  -- Stress3 Election & Write Metrics --\n";
    std::cout << "    成功选举:      " << successful_elections << "/" << kCycles << "\n";
    std::cout << "    选举 P50:      " << std::fixed << std::setprecision(0)
              << election_lat.P50() << " ms\n";
    std::cout << "    选举 P99:      " << std::fixed << std::setprecision(0)
              << election_lat.P99() << " ms\n";
    std::cout << "    写入 P50:      " << std::fixed << std::setprecision(2)
              << write_lat.P50() << " ms\n";
    std::cout << "    写入 P99:      " << std::fixed << std::setprecision(2)
              << write_lat.P99() << " ms\n";

    (void)system("rm -rf /tmp/stress3_n1 /tmp/stress3_n2 /tmp/stress3_n3");
    std::cout << "Stress 3 PASSED\n";
}

// ===================================================================
// Stress Test 4: Network Partition and Recovery
// ===================================================================
static void StressPartitionAndRecovery() {
    std::cout << "\n===== Stress 4: Network Partition and Recovery =====\n";
    (void)system("rm -rf /tmp/stress4_n1 /tmp/stress4_n2 /tmp/stress4_n3");

    std::vector<std::string> peers = {
        "127.0.0.1:21031", "127.0.0.1:21032", "127.0.0.1:21033",
    };

    RaftConfig cfg1; cfg1.node_id = peers[0]; cfg1.peers = peers;
    cfg1.data_dir = "/tmp/stress4_n1"; cfg1.election_timeout_ms = 1500;
    RaftConfig cfg2; cfg2.node_id = peers[1]; cfg2.peers = peers;
    cfg2.data_dir = "/tmp/stress4_n2"; cfg2.election_timeout_ms = 5000;
    RaftConfig cfg3; cfg3.node_id = peers[2]; cfg3.peers = peers;
    cfg3.data_dir = "/tmp/stress4_n3"; cfg3.election_timeout_ms = 5000;

    RaftNode n1(cfg1), n2(cfg2), n3(cfg3);
    std::vector<RaftNode*> nodes = {&n1, &n2, &n3};

    WireNodes(nodes, peers);
    n1.Start(); n2.Start(); n3.Start();
    Sleep(3000);

    assert(n1.IsLeader());
    std::cout << "Initial leader: n1\n";

    // Write data pre-partition
    LatencyRecorder pre_lat;
    double pre_start = NowMs();
    for (int i = 0; i < 10; i++) {
        double t0 = NowMs();
        auto r = n1.Propose(MakeCommand("/stress4/pre" + std::to_string(i)), 3000);
        pre_lat.Record(NowMs() - t0);
        assert(r.committed);
    }
    double pre_elapsed = NowMs() - pre_start;
    std::cout << "Pre-partition: 10 entries committed\n";
    std::cout << "  写入 QPS: " << std::fixed << std::setprecision(1)
              << (10.0 / (pre_elapsed / 1000.0)) << "  P99: "
              << pre_lat.P99() << " ms\n";

    // Partition: isolate n1
    n1.SetSendRPCCallback([&](const std::string&, const RaftMessage&) {});
    n2.SetSendRPCCallback([&](const std::string& peer_id, const RaftMessage& msg) {
        if (peer_id == peers[2]) {
            auto* resp = n3.HandleAppendEntries(
                msg.append_entries().leader_id(), msg.append_entries());
            if (resp) n2.HandleAppendEntriesResponse(peer_id, *resp);
        } else if (peer_id == peers[0]) {
            auto* resp = n1.HandleRequestVote(
                msg.request_vote().candidate_id(), msg.request_vote());
            if (resp) n2.HandleRequestVoteResponse(peer_id, *resp);
        }
    });
    n3.SetSendRPCCallback([&](const std::string& peer_id, const RaftMessage& msg) {
        if (peer_id == peers[1]) {
            auto* resp = n2.HandleAppendEntries(
                msg.append_entries().leader_id(), msg.append_entries());
            if (resp) n3.HandleAppendEntriesResponse(peer_id, *resp);
        }
    });

    std::cout << "Partition created: n1 isolated\n";

    double heal_start = NowMs();
    Sleep(6000);

    bool n2_leader = n2.IsLeader();
    bool n3_leader = n3.IsLeader();
    double partition_duration = NowMs() - heal_start;
    std::cout << "After partition (" << partition_duration << "ms): n2_leader="
              << n2_leader << " n3_leader=" << n3_leader << "\n";

    // Heal
    WireNodes(nodes, peers);
    double heal_elapsed_start = NowMs();
    std::cout << "Partition healed\n";
    Sleep(6000);
    double heal_time = NowMs() - heal_elapsed_start;

    int leaders = 0;
    if (n1.IsLeader()) { leaders++; std::cout << "  n1 is leader\n"; }
    if (n2.IsLeader()) { leaders++; std::cout << "  n2 is leader\n"; }
    if (n3.IsLeader()) { leaders++; std::cout << "  n3 is leader\n"; }
    assert(leaders == 1);

    RaftNode* leader = nullptr;
    if (n1.IsLeader()) leader = &n1;
    else if (n2.IsLeader()) leader = &n2;
    else leader = &n3;

    LatencyRecorder post_lat;
    double post_start = NowMs();
    for (int i = 0; i < 5; i++) {
        double t0 = NowMs();
        auto r = leader->Propose(MakeCommand("/stress4/post" + std::to_string(i)), 3000);
        post_lat.Record(NowMs() - t0);
        assert(r.committed);
    }
    double post_elapsed = NowMs() - post_start;
    std::cout << "Post-heal: 5 entries committed\n";

    std::cout << "\n  -- Stress4 Partition Metrics --\n";
    std::cout << "    分区持续时间: " << partition_duration << " ms\n";
    std::cout << "    恢复时间:      " << heal_time << " ms\n";
    std::cout << "    恢复后写入 QPS: " << std::fixed << std::setprecision(1)
              << (5.0 / (post_elapsed / 1000.0)) << "  P99: "
              << post_lat.P99() << " ms\n";

    Sleep(2000);
    std::cout << "Log sizes: n1=" << n1.GetLogSize()
              << " n2=" << n2.GetLogSize()
              << " n3=" << n3.GetLogSize() << "\n";

    n1.Stop(); n2.Stop(); n3.Stop();
    (void)system("rm -rf /tmp/stress4_n1 /tmp/stress4_n2 /tmp/stress4_n3");
    std::cout << "Stress 4 PASSED\n";
}

// ===================================================================
// Stress Test 5: Large Payload
// ===================================================================
static void StressLargePayload() {
    std::cout << "\n===== Stress 5: Large Payload (up to 256KB commands) =====\n";
    (void)system("rm -rf /tmp/stress5_n1");

    RaftConfig cfg;
    cfg.node_id   = "127.0.0.1:21041";
    cfg.peers     = {"127.0.0.1:21041"};
    cfg.data_dir  = "/tmp/stress5_n1";
    cfg.election_timeout_ms = 400;

    RaftNode node(cfg);
    std::atomic<int> applied{0};
    node.SetApplyCallback([&](uint64_t, const std::string&) { applied++; });
    node.Start();
    Sleep(1500);
    assert(node.IsLeader());

    std::vector<size_t> sizes = {1, 64, 256, 1024, 4096, 16384, 65536, 262144};

    std::cout << "\n  -- Stress5 Payload Latency --\n";
    std::cout << "  " << std::setw(10) << "大小(bytes)"
              << std::setw(12) << "延迟(ms)"
              << std::setw(14) << "吞吐(MB/s)" << "\n";
    std::cout << "  " << std::string(36, '-') << "\n";

    for (auto size : sizes) {
        std::string payload(size, 'X');
        // Warm up one
        node.Propose(payload, 5000);

        // Measure 3 iterations and take median
        LatencyRecorder rec;
        for (int r = 0; r < 3; r++) {
            double t0 = NowMs();
            auto result = node.Propose(payload, 5000);
            rec.Record(NowMs() - t0);
            assert(result.committed);
        }
        double lat_ms = rec.P50();
        double mbps = (size / 1024.0 / 1024.0) / (lat_ms / 1000.0);
        std::cout << "  " << std::setw(10) << size
                  << std::setw(12) << std::fixed << std::setprecision(2) << lat_ms
                  << std::setw(14) << std::fixed << std::setprecision(2) << mbps << "\n";
    }

    assert((int)applied == (int)sizes.size() * 4);  // warmup + 3 measured

    node.Stop();
    (void)system("rm -rf /tmp/stress5_n1");
    std::cout << "Stress 5 PASSED\n";
}

// ===================================================================
// Stress Test 6: Persistence Under Stress
// ===================================================================
static void StressPersistenceUnderStress() {
    std::cout << "\n===== Stress 6: Persistence Under Stress =====\n";
    (void)system("rm -rf /tmp/stress6_n1");

    const int kEntries = 500;
    LatencyRecorder phase1_lat, phase2_lat;

    // Phase 1: Write many entries
    {
        RaftConfig cfg;
        cfg.node_id   = "127.0.0.1:21051";
        cfg.peers     = {"127.0.0.1:21051"};
        cfg.data_dir  = "/tmp/stress6_n1";
        cfg.election_timeout_ms = 400;

        RaftNode node(cfg);
        std::atomic<int> applied{0};
        node.SetApplyCallback([&](uint64_t, const std::string&) { applied++; });
        node.Start();
        Sleep(1500);
        assert(node.IsLeader());

        double start = NowMs();
        for (int i = 0; i < kEntries; i++) {
            double t0 = NowMs();
            auto r = node.Propose(MakeCommand("/stress6/file" + std::to_string(i)), 5000);
            phase1_lat.Record(NowMs() - t0);
            assert(r.committed);
        }
        double elapsed = NowMs() - start;
        MetricsReporter::PrintWriteStats("Phase1 (initial write)", phase1_lat, elapsed);
        assert((int)applied == kEntries);
        node.Stop();
    }

    // Phase 2: Restart and verify
    {
        RaftConfig cfg;
        cfg.node_id   = "127.0.0.1:21051";
        cfg.peers     = {"127.0.0.1:21051"};
        cfg.data_dir  = "/tmp/stress6_n1";
        cfg.election_timeout_ms = 400;

        RaftNode node(cfg);
        std::atomic<int> applied{0};
        node.SetApplyCallback([&](uint64_t, const std::string&) { applied++; });
        node.Start();
        Sleep(1500);

        assert(node.IsLeader());
        assert(node.GetLogSize() == (size_t)kEntries);
        std::cout << "  Restored: log_size=" << node.GetLogSize()
                  << " term=" << node.GetCurrentTerm() << "\n";

        double start = NowMs();
        for (int i = kEntries; i < kEntries + 100; i++) {
            double t0 = NowMs();
            auto r = node.Propose(MakeCommand("/stress6/file" + std::to_string(i)), 5000);
            phase2_lat.Record(NowMs() - t0);
            assert(r.committed);
        }
        double elapsed = NowMs() - start;
        MetricsReporter::PrintWriteStats("Phase2 (after restart)", phase2_lat, elapsed);
        assert(node.GetLogSize() == (size_t)(kEntries + 100));
        node.Stop();
    }

    // Phase 3: Restart again and verify all data
    {
        RaftConfig cfg;
        cfg.node_id   = "127.0.0.1:21051";
        cfg.peers     = {"127.0.0.1:21051"};
        cfg.data_dir  = "/tmp/stress6_n1";
        cfg.election_timeout_ms = 400;

        RaftNode node(cfg);
        node.Start();
        Sleep(1500);
        assert(node.IsLeader());
        assert(node.GetLogSize() == (size_t)(kEntries + 100));
        std::cout << "  Final restore: log_size=" << node.GetLogSize()
                  << " commit_index=" << node.GetCommitIndex() << "\n";
        node.Stop();
    }

    (void)system("rm -rf /tmp/stress6_n1");
    std::cout << "Stress 6 PASSED\n";
}

// ===================================================================
// Stress Test 7: Rapid Election Churn
// ===================================================================
static void StressRapidElection() {
    std::cout << "\n===== Stress 7: Rapid Election with Fast Timeouts =====\n";
    (void)system("rm -rf /tmp/stress7_n1 /tmp/stress7_n2 /tmp/stress7_n3");

    std::vector<std::string> peers = {
        "127.0.0.1:21061", "127.0.0.1:21062", "127.0.0.1:21063",
    };

    RaftConfig cfg1; cfg1.node_id = peers[0]; cfg1.peers = peers;
    cfg1.data_dir = "/tmp/stress7_n1"; cfg1.election_timeout_ms = 300;
    cfg1.heartbeat_interval_ms = 100;
    RaftConfig cfg2; cfg2.node_id = peers[1]; cfg2.peers = peers;
    cfg2.data_dir = "/tmp/stress7_n2"; cfg2.election_timeout_ms = 350;
    RaftConfig cfg3; cfg3.node_id = peers[2]; cfg3.peers = peers;
    cfg3.data_dir = "/tmp/stress7_n3"; cfg3.election_timeout_ms = 400;

    RaftNode n1(cfg1), n2(cfg2), n3(cfg3);
    std::vector<RaftNode*> nodes = {&n1, &n2, &n3};
    WireNodes(nodes, peers);

    n1.Start(); n2.Start(); n3.Start();

    std::cout << "Letting elections churn for 10 seconds...\n";
    double churn_start = NowMs();
    Sleep(10000);
    double churn_elapsed = NowMs() - churn_start;

    int leaders = 0;
    RaftNode* leader = nullptr;
    if (n1.IsLeader()) { leaders++; leader = &n1; }
    if (n2.IsLeader()) { leaders++; leader = &n2; }
    if (n3.IsLeader()) { leaders++; leader = &n3; }
    assert(leaders == 1);
    std::cout << "Final state: 1 leader (" << leader->GetLeaderID()
              << "), term=" << leader->GetCurrentTerm()
              << ", total_terms_seen=" << leader->GetCurrentTerm() << "\n";

    LatencyRecorder post_churn_lat;
    double post_start = NowMs();
    for (int i = 0; i < 20; i++) {
        double t0 = NowMs();
        auto r = leader->Propose(MakeCommand("/stress7/file" + std::to_string(i)), 5000);
        post_churn_lat.Record(NowMs() - t0);
        assert(r.committed);
    }
    double post_elapsed = NowMs() - post_start;

    std::cout << "\n  -- Stress7 Churn & Recovery Metrics --\n";
    std::cout << "    震荡时长:      " << churn_elapsed / 1000.0 << " s\n";
    std::cout << "    最终 term:     " << leader->GetCurrentTerm() << "\n";
    MetricsReporter::PrintWriteStats("Post-churn writes", post_churn_lat, post_elapsed);

    n1.Stop(); n2.Stop(); n3.Stop();
    (void)system("rm -rf /tmp/stress7_n1 /tmp/stress7_n2 /tmp/stress7_n3");
    std::cout << "Stress 7 PASSED\n";
}

// ===================================================================
// Stress Test 8: HA NameNode End-to-End Stress
// ===================================================================
static void StressHANameNodeEndToEnd() {
    std::cout << "\n===== Stress 8: HA NameNode End-to-End Stress =====\n";
    (void)system("rm -rf /tmp/stress8_nn1 /tmp/stress8_nn2 /tmp/stress8_nn3");
    (void)system("rm -rf /tmp/stress8_dn1 /tmp/stress8_dn2 /tmp/stress8_dn3");

    std::vector<std::string> peers = {
        "127.0.0.1:21072", "127.0.0.1:21082", "127.0.0.1:21092",
    };

    HANameNodeServer nn1("/tmp/stress8_nn1", "127.0.0.1", 21071, "127.0.0.1", 21072, peers, 2);
    HANameNodeServer nn2("/tmp/stress8_nn2", "127.0.0.1", 21081, "127.0.0.1", 21082, peers, 2);
    HANameNodeServer nn3("/tmp/stress8_nn3", "127.0.0.1", 21091, "127.0.0.1", 21092, peers, 2);

    assert(nn1.Start() && nn2.Start() && nn3.Start());
    std::cout << "3 HA NameNodes started\n";
    Sleep(5000);

    int leader_port = 0;
    if (nn1.IsLeader()) leader_port = 21071;
    else if (nn2.IsLeader()) leader_port = 21081;
    else if (nn3.IsLeader()) leader_port = 21091;
    assert(leader_port > 0);
    std::cout << "Leader on client port " << leader_port << "\n";

    DataNodeServer dn1("/tmp/stress8_dn1", "127.0.0.1", 21073, "127.0.0.1", leader_port);
    DataNodeServer dn2("/tmp/stress8_dn2", "127.0.0.1", 21083, "127.0.0.1", leader_port);
    DataNodeServer dn3("/tmp/stress8_dn3", "127.0.0.1", 21093, "127.0.0.1", leader_port);
    assert(dn1.Start() && dn2.Start() && dn3.Start());
    Sleep(1000);
    std::cout << "3 DataNodes connected\n";

    StorageClient client("127.0.0.1", leader_port);
    const int kFiles = 50;

    // --- Write phase ---
    LatencyRecorder write_lat;
    int success = 0;
    double write_start = NowMs();
    for (int i = 0; i < kFiles; i++) {
        std::ostringstream path;
        path << "/stress8/file" << std::setw(3) << std::setfill('0') << i << ".dat";
        std::string content(4096, 'A' + (i % 26));
        double t0 = NowMs();
        if (client.PutFile(path.str(), content)) success++;
        write_lat.Record(NowMs() - t0);
        if (i > 0 && i % 10 == 0) {
            std::cout << "  Written " << i << "/" << kFiles << " files\n";
        }
    }
    double write_elapsed = NowMs() - write_start;
    assert(success == kFiles);

    MetricsReporter::PrintWriteStats("HA NameNode PUT", write_lat, write_elapsed);

    // --- Read phase ---
    LatencyRecorder read_lat;
    int read_ok = 0;
    double read_start = NowMs();
    for (int i = 0; i < kFiles; i++) {
        std::ostringstream path;
        path << "/stress8/file" << std::setw(3) << std::setfill('0') << i << ".dat";
        std::string data;
        double t0 = NowMs();
        bool ok = client.GetFile(path.str(), &data);
        double lat = NowMs() - t0;
        read_lat.Record(lat);
        if (ok) {
            std::string expected(4096, 'A' + (i % 26));
            if (data == expected) read_ok++;
        }
    }
    double read_elapsed = NowMs() - read_start;
    assert(read_ok == kFiles);

    MetricsReporter::PrintReadStats("HA NameNode GET", read_lat, read_elapsed);

    // --- Summary ---
    std::cout << "\n  -- Stress8 End-to-End Summary --\n";
    std::cout << "    PUT QPS:      " << std::fixed << std::setprecision(1)
              << (kFiles / (write_elapsed / 1000.0)) << "\n";
    std::cout << "    GET QPS:      " << std::fixed << std::setprecision(1)
              << (kFiles / (read_elapsed / 1000.0)) << "\n";
    std::cout << "    PUT P99:      " << std::fixed << std::setprecision(2)
              << write_lat.P99() << " ms\n";
    std::cout << "    GET P99:      " << std::fixed << std::setprecision(2)
              << read_lat.P99() << " ms\n";
    std::cout << "    数据正确性:   " << read_ok << "/" << kFiles << "\n";

    dn1.Stop(); dn2.Stop(); dn3.Stop();
    nn1.Stop(); nn2.Stop(); nn3.Stop();
    (void)system("rm -rf /tmp/stress8_nn1 /tmp/stress8_nn2 /tmp/stress8_nn3");
    (void)system("rm -rf /tmp/stress8_dn1 /tmp/stress8_dn2 /tmp/stress8_dn3");
    std::cout << "Stress 8 PASSED\n";
}

// ===================================================================
// main
// ===================================================================
int main() {
    std::cout << "================================================\n";
    std::cout << "  Raft Stress Test Suite\n";
    std::cout << "================================================\n";

    StressMassiveLogReplication();
    StressConcurrentProposals();
    StressLeaderChurn();
    StressPartitionAndRecovery();
    StressLargePayload();
    StressPersistenceUnderStress();
    StressRapidElection();
    StressHANameNodeEndToEnd();

    std::cout << "\n================================================\n";
    std::cout << "  All Raft Stress Tests PASSED!\n";
    std::cout << "================================================\n";
    return 0;
}
