#pragma once
#include "proto/raft.pb.h"
#include <string>
#include <vector>
#include <functional>
#include <memory>
#include <atomic>
#include <mutex>
#include <thread>
#include <chrono>
#include <random>
#include <condition_variable>

namespace mini_storage {

enum class RaftState {
    FOLLOWER,
    CANDIDATE,
    LEADER
};

struct RaftConfig {
    std::string node_id;
    std::vector<std::string> peers;
    std::string data_dir;
    int election_timeout_ms   = 3000;
    int heartbeat_interval_ms = 500;
    int snapshot_interval     = 10000;
};

struct LogEntry {
    uint64_t term    = 0;
    uint64_t index   = 0;
    std::string command;
};

using ApplyCallback    = std::function<void(uint64_t index, const std::string& command)>;
using SendRPCCallback  = std::function<void(const std::string& peer_id, const RaftMessage& msg)>;
using SnapshotCallback = std::function<std::string()>;
using RestoreCallback  = std::function<bool(const std::string& snapshot_data)>;

class RaftNode {
public:
    RaftNode(const RaftConfig& config);
    ~RaftNode();

    bool Start();
    void Stop();

    struct ProposeResult {
        uint64_t index;
        bool     committed;
        bool     is_leader;
        std::string leader_id;
    };
    ProposeResult Propose(const std::string& command, int timeout_ms = 5000);

    // Query
    bool IsLeader() const;
    std::string GetLeaderID() const;
    RaftState GetState() const;
    uint64_t GetCurrentTerm() const;
    uint64_t GetCommitIndex() const;
    uint64_t GetLastLogIndex() const;
    uint64_t GetLastLogTerm() const;
    size_t   GetLogSize() const;

    // Callbacks
    void SetApplyCallback(ApplyCallback cb)       { apply_cb_ = std::move(cb); }
    void SetSendRPCCallback(SendRPCCallback cb)   { send_rpc_cb_ = std::move(cb); }
    void SetSnapshotCallback(SnapshotCallback cb) { snapshot_cb_ = std::move(cb); }
    void SetRestoreCallback(RestoreCallback cb)   { restore_cb_ = std::move(cb); }

    // Timer
    void OnElectionTimeout();

    // Snapshot
    bool TakeSnapshot();
    bool RestoreFromSnapshot();

    // === RPC processing (called by RaftRPC) ===
    // These handle incoming messages and return the response to be sent

    // Handle an incoming request. If non-null response is returned, caller sends it back.
    RequestVoteResponse* HandleRequestVote(const std::string& from,
                                            const RequestVoteRequest& req);
    AppendEntriesResponse* HandleAppendEntries(const std::string& from,
                                                const AppendEntriesRequest& req);
    InstallSnapshotResponse* HandleInstallSnapshot(const std::string& from,
                                                     const InstallSnapshotRequest& req);

    // Handle response messages (arriving from peers we sent requests to)
    void HandleRequestVoteResponse(const std::string& from,
                                    const RequestVoteResponse& resp);
    void HandleAppendEntriesResponse(const std::string& from,
                                      const AppendEntriesResponse& resp);

    // Get the number of peers in cluster
    int PeerCount() const { return (int)config_.peers.size(); }

    // Helper: is a peer one of ours?
    bool IsPeer(const std::string& id) const {
        for (const auto& p : config_.peers)
            if (p == id) return true;
        return false;
    }

    // Serialize current MetadataStore state for snapshot
    std::string GetSnapshotData() const;

private:
    void BecomeFollower(uint64_t term);
    void BecomeCandidate();
    void BecomeLeader();
    void StartElection();
    void SendHeartbeats();
    void AdvanceCommitIndex();
    void ApplyCommitted(uint64_t up_to_index);
    LogEntry* GetLogEntry(uint64_t index);
    const LogEntry* GetLogEntry(uint64_t index) const;
    bool PersistState();
    bool RestoreState();
    std::string StateFilePath() const;
    std::string SnapshotFilePath() const;
    void ResetElectionTimer();
    void TimerLoop();
    int  RandomElectionTimeout() const;

    RaftConfig config_;
    uint64_t current_term_{0};
    std::string voted_for_;
    std::vector<LogEntry> log_;
    uint64_t commit_index_{0};
    uint64_t last_applied_{0};
    std::unordered_map<std::string, uint64_t> next_index_;
    std::unordered_map<std::string, uint64_t> match_index_;
    uint64_t snapshot_last_index_{0};
    uint64_t snapshot_last_term_{0};

    RaftState state_{RaftState::FOLLOWER};
    std::string leader_id_;
    std::string my_id_;

    // Vote counting (for candidate state)
    int votes_received_{0};

    std::atomic<bool> stop_{false};
    std::thread timer_thread_;
    std::chrono::steady_clock::time_point election_deadline_;
    mutable std::mutex timer_mutex_;

    mutable std::recursive_mutex state_mutex_;
    std::condition_variable propose_cv_;
    std::mutex propose_mutex_;

    ApplyCallback    apply_cb_;
    SendRPCCallback  send_rpc_cb_;
    SnapshotCallback snapshot_cb_;
    RestoreCallback  restore_cb_;

    // Pool of response objects (reused to avoid allocation)
    RequestVoteResponse      vote_resp_;
    AppendEntriesResponse    ae_resp_;
    InstallSnapshotResponse  snap_resp_;
};

}  // namespace mini_storage
