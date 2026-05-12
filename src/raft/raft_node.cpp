#include "raft_node.h"
#include <fstream>
#include <iostream>
#include <algorithm>
#include <filesystem>

namespace fs = std::filesystem;

namespace mini_storage {

RaftNode::RaftNode(const RaftConfig& config)
    : config_(config), my_id_(config.node_id)
{
    fs::create_directories(config_.data_dir);
    LogEntry dummy;
    dummy.term  = 0;
    dummy.index = 0;
    log_.push_back(dummy);

    // Default no-op callbacks
    send_rpc_cb_ = [](const std::string&, const RaftMessage&) {};
}

RaftNode::~RaftNode() { Stop(); }

bool RaftNode::Start() {
    RestoreState();
    RestoreFromSnapshot();
    BecomeFollower(current_term_);
    stop_.store(false);
    ResetElectionTimer();
    timer_thread_ = std::thread(&RaftNode::TimerLoop, this);
    std::cout << "[Raft " << my_id_ << "] Started term=" << current_term_
              << " log_size=" << (log_.size() - 1) << std::endl;
    return true;
}

void RaftNode::Stop() {
    stop_.store(true);
    if (timer_thread_.joinable()) timer_thread_.join();
}

// ===== State Transitions =====

void RaftNode::BecomeFollower(uint64_t term) {
    std::lock_guard<std::recursive_mutex> lock(state_mutex_);
    if (term > current_term_) {
        current_term_ = term;
        voted_for_.clear();
        PersistState();
    }
    state_ = RaftState::FOLLOWER;
    leader_id_.clear();
    std::cout << "[Raft " << my_id_ << "] BecomeFollower term=" << current_term_ << std::endl;
}

void RaftNode::BecomeCandidate() {
    std::lock_guard<std::recursive_mutex> lock(state_mutex_);
    state_ = RaftState::CANDIDATE;
    current_term_++;
    voted_for_ = my_id_;
    leader_id_.clear();
    votes_received_ = 1;  // Vote for self
    PersistState();
    std::cout << "[Raft " << my_id_ << "] BecomeCandidate term=" << current_term_ << std::endl;
}

void RaftNode::BecomeLeader() {
    {
        std::lock_guard<std::recursive_mutex> lock(state_mutex_);
        state_     = RaftState::LEADER;
        leader_id_ = my_id_;
        uint64_t last_log_index = log_.back().index;
        next_index_.clear();
        match_index_.clear();
        for (const auto& peer : config_.peers) {
            if (peer != my_id_) {
                next_index_[peer]  = last_log_index + 1;
                match_index_[peer] = 0;
            }
        }
    }
    SendHeartbeats();
    ResetElectionTimer();
    std::cout << "[Raft " << my_id_ << "] BecomeLeader term=" << current_term_
              << " log_size=" << (log_.size() - 1) << std::endl;
}

// ===== Election =====

void RaftNode::StartElection() {
    uint64_t last_log_index;
    uint64_t last_log_term;
    uint64_t term;
    {
        std::lock_guard<std::recursive_mutex> lock(state_mutex_);
        last_log_index = log_.back().index;
        last_log_term  = log_.back().term;
        term           = current_term_;
    }

    RequestVoteRequest req;
    req.set_term(term);
    req.set_candidate_id(my_id_);
    req.set_last_log_index(last_log_index);
    req.set_last_log_term(last_log_term);

    RaftMessage msg;
    msg.set_type(RaftMessage::REQUEST_VOTE);
    *msg.mutable_request_vote() = req;

    for (const auto& peer : config_.peers) {
        if (peer != my_id_) {
            send_rpc_cb_(peer, msg);
        }
    }

    // Check if we already have a majority (e.g., single-node cluster)
    bool should_lead = false;
    {
        std::lock_guard<std::recursive_mutex> lock(state_mutex_);
        int majority = (int)config_.peers.size() / 2 + 1;
        if (state_ == RaftState::CANDIDATE && votes_received_ >= majority) {
            should_lead = true;
        }
    }
    if (should_lead) {
        BecomeLeader();
    }
}

RequestVoteResponse* RaftNode::HandleRequestVote(const std::string& from,
                                                   const RequestVoteRequest& req) {
    std::lock_guard<std::recursive_mutex> lock(state_mutex_);
    vote_resp_.set_term(current_term_);

    if (req.term() < current_term_) {
        vote_resp_.set_vote_granted(false);
        return &vote_resp_;
    }

    if (req.term() > current_term_) {
        current_term_ = req.term();
        state_        = RaftState::FOLLOWER;
        voted_for_.clear();
        leader_id_.clear();
        PersistState();
        vote_resp_.set_term(current_term_);
    }

    bool can_vote = false;
    if (voted_for_.empty() || voted_for_ == req.candidate_id()) {
        uint64_t my_last_term  = log_.back().term;
        uint64_t my_last_index = log_.back().index;
        if (req.last_log_term() > my_last_term ||
            (req.last_log_term() == my_last_term && req.last_log_index() >= my_last_index))
            can_vote = true;
    }

    if (can_vote) {
        voted_for_ = req.candidate_id();
        PersistState();
        vote_resp_.set_vote_granted(true);
        ResetElectionTimer();
        std::cout << "[Raft " << my_id_ << "] Voted for " << req.candidate_id()
                  << " in term " << current_term_ << std::endl;
    } else {
        vote_resp_.set_vote_granted(false);
    }
    return &vote_resp_;
}

void RaftNode::HandleRequestVoteResponse(const std::string& from,
                                           const RequestVoteResponse& resp) {
    std::lock_guard<std::recursive_mutex> lock(state_mutex_);

    if (resp.term() > current_term_) {
        current_term_ = resp.term();
        state_        = RaftState::FOLLOWER;
        voted_for_.clear();
        leader_id_.clear();
        PersistState();
        return;
    }

    if (state_ != RaftState::CANDIDATE) return;
    if (resp.term() < current_term_) return;

    if (resp.vote_granted()) {
        votes_received_++;
        std::cout << "[Raft " << my_id_ << "] Got vote from " << from
                  << " (" << votes_received_ << "/" << config_.peers.size() << ")\n";

        int majority = (int)config_.peers.size() / 2 + 1;
        if (votes_received_ >= majority) {
            BecomeLeader();
        }
    }
}

// ===== Log Replication =====

AppendEntriesResponse* RaftNode::HandleAppendEntries(const std::string& from,
                                                       const AppendEntriesRequest& req) {
    std::lock_guard<std::recursive_mutex> lock(state_mutex_);
    ae_resp_.set_term(current_term_);

    if (req.term() < current_term_) {
        ae_resp_.set_success(false);
        return &ae_resp_;
    }

    if (req.term() > current_term_) {
        current_term_ = req.term();
        state_        = RaftState::FOLLOWER;
        voted_for_.clear();
        PersistState();
        ae_resp_.set_term(current_term_);
    }

    leader_id_ = from;
    state_     = RaftState::FOLLOWER;
    ResetElectionTimer();

    uint64_t prev_log_index = req.prev_log_index();
    uint64_t prev_log_term  = req.prev_log_term();

    if (prev_log_index > log_.back().index) {
        ae_resp_.set_success(false);
        ae_resp_.set_match_index(log_.back().index);
        return &ae_resp_;
    }

    if (prev_log_index >= snapshot_last_index_) {
        const LogEntry* prev = GetLogEntry(prev_log_index);
        if (prev && prev->term != prev_log_term) {
            // Conflict: truncate and return match_index for fast backtrack
            for (uint64_t idx = prev_log_index; idx <= log_.back().index; idx++) {
                const LogEntry* e = GetLogEntry(idx);
                if (e && e->term != prev_log_term) {
                    while (log_.back().index >= idx) log_.pop_back();
                    break;
                }
            }
            ae_resp_.set_success(false);
            ae_resp_.set_match_index(log_.back().index);
            PersistState();
            return &ae_resp_;
        }
    } else {
        // prev_log_index < snapshot_last_index_
        if (prev_log_index != snapshot_last_index_ || prev_log_term != snapshot_last_term_) {
            ae_resp_.set_success(false);
            ae_resp_.set_match_index(snapshot_last_index_);
            return &ae_resp_;
        }
    }

    // Append new entries
    for (int i = 0; i < req.entries_size(); i++) {
        const auto& entry = req.entries(i);
        uint64_t entry_index = entry.index();
        if (entry_index > log_.back().index) {
            LogEntry le;
            le.term    = entry.term();
            le.index   = entry.index();
            le.command = entry.command();
            log_.push_back(le);
        }
    }
    PersistState();

    if (req.leader_commit() > commit_index_) {
        uint64_t new_commit = std::min(req.leader_commit(), log_.back().index);
        if (new_commit > commit_index_) {
            commit_index_ = new_commit;
            ApplyCommitted(commit_index_);
        }
    }

    ae_resp_.set_success(true);
    ae_resp_.set_match_index(log_.back().index);
    return &ae_resp_;
}

void RaftNode::HandleAppendEntriesResponse(const std::string& from,
                                             const AppendEntriesResponse& resp) {
    std::lock_guard<std::recursive_mutex> lock(state_mutex_);

    if (resp.term() > current_term_) {
        current_term_ = resp.term();
        state_        = RaftState::FOLLOWER;
        voted_for_.clear();
        leader_id_.clear();
        PersistState();
        return;
    }

    if (state_ != RaftState::LEADER) return;

    if (resp.success()) {
        match_index_[from] = resp.match_index();
        next_index_[from]  = resp.match_index() + 1;
        AdvanceCommitIndex();
    } else {
        // Decrement next_index for fast backtrack
        uint64_t match = resp.match_index();
        if (match < next_index_[from]) {
            next_index_[from] = std::max(match + 1, snapshot_last_index_ + 1);
            if (next_index_[from] < 1) next_index_[from] = 1;
        }
    }

    // Notify any waiting propose
    propose_cv_.notify_all();
}

void RaftNode::SendHeartbeats() {
    std::lock_guard<std::recursive_mutex> lock(state_mutex_);
    if (state_ != RaftState::LEADER) return;

    for (const auto& peer : config_.peers) {
        if (peer == my_id_) continue;
        uint64_t next_idx       = next_index_[peer];
        uint64_t prev_log_index = next_idx - 1;
        uint64_t prev_log_term  = 0;

        if (prev_log_index >= snapshot_last_index_) {
            const LogEntry* prev = GetLogEntry(prev_log_index);
            if (prev) prev_log_term = prev->term;
        } else if (prev_log_index == snapshot_last_index_) {
            prev_log_term = snapshot_last_term_;
        }

        AppendEntriesRequest req;
        req.set_term(current_term_);
        req.set_leader_id(my_id_);
        req.set_prev_log_index(prev_log_index);
        req.set_prev_log_term(prev_log_term);
        req.set_leader_commit(commit_index_);

        for (size_t i = next_idx; i < log_.size(); i++) {
            auto* entry = req.add_entries();
            entry->set_term(log_[i].term);
            entry->set_index(log_[i].index);
            entry->set_command(log_[i].command);
        }

        RaftMessage msg;
        msg.set_type(RaftMessage::APPEND_ENTRIES);
        *msg.mutable_append_entries() = req;
        send_rpc_cb_(peer, msg);
    }
}

// ===== Commit & Apply =====

void RaftNode::AdvanceCommitIndex() {
    uint64_t last_log_index = log_.back().index;
    for (uint64_t n = last_log_index; n > commit_index_; n--) {
        const LogEntry* entry = GetLogEntry(n);
        if (!entry || entry->term != current_term_) continue;
        int count = 1;
        for (const auto& peer : config_.peers) {
            if (peer == my_id_) continue;
            auto it = match_index_.find(peer);
            if (it != match_index_.end() && it->second >= n) count++;
        }
        int majority = (int)config_.peers.size() / 2 + 1;
        if (count >= majority) {
            commit_index_ = n;
            ApplyCommitted(commit_index_);
            propose_cv_.notify_all();
            break;
        }
    }
}

void RaftNode::ApplyCommitted(uint64_t up_to_index) {
    while (last_applied_ < up_to_index) {
        last_applied_++;
        const LogEntry* entry = GetLogEntry(last_applied_);
        if (entry && !entry->command.empty() && apply_cb_) {
            apply_cb_(last_applied_, entry->command);
        }
    }
}

// ===== Client Propose =====

RaftNode::ProposeResult RaftNode::Propose(const std::string& command, int timeout_ms) {
    ProposeResult result;
    result.committed = false;
    result.is_leader = false;

    {
        std::lock_guard<std::recursive_mutex> lock(state_mutex_);
        if (state_ != RaftState::LEADER) {
            result.leader_id = leader_id_;
            return result;
        }
        result.is_leader = true;
        LogEntry entry;
        entry.term    = current_term_;
        entry.index   = log_.back().index + 1;
        entry.command = command;
        log_.push_back(entry);
        PersistState();
        match_index_[my_id_] = entry.index;
        next_index_[my_id_]  = entry.index + 1;
        result.index = entry.index;
    }

    // Try to advance commit immediately (needed for single-node clusters)
    {
        std::lock_guard<std::recursive_mutex> lock(state_mutex_);
        AdvanceCommitIndex();
    }

    SendHeartbeats();

    {
        std::unique_lock<std::mutex> lock(propose_mutex_);
        auto deadline = std::chrono::steady_clock::now()
                        + std::chrono::milliseconds(timeout_ms);
        while (true) {
            {
                std::lock_guard<std::recursive_mutex> sl(state_mutex_);
                if (commit_index_ >= result.index) {
                    result.committed = true;
                    break;
                }
                if (state_ != RaftState::LEADER) {
                    result.is_leader = false;
                    result.leader_id = leader_id_;
                    break;
                }
            }
            if (propose_cv_.wait_until(lock, deadline) == std::cv_status::timeout)
                break;
        }
    }
    return result;
}

// ===== Snapshot RPC =====

InstallSnapshotResponse* RaftNode::HandleInstallSnapshot(const std::string& from,
                                                           const InstallSnapshotRequest& req) {
    std::lock_guard<std::recursive_mutex> lock(state_mutex_);
    snap_resp_.set_term(current_term_);

    if (req.term() < current_term_) {
        snap_resp_.set_success(false);
        return &snap_resp_;
    }

    if (req.term() > current_term_) {
        current_term_ = req.term();
        PersistState();
        snap_resp_.set_term(current_term_);
    }

    leader_id_ = from;
    state_     = RaftState::FOLLOWER;
    ResetElectionTimer();

    if (req.last_included_index() <= snapshot_last_index_ ||
        req.last_included_index() <= commit_index_) {
        snap_resp_.set_success(true);
        return &snap_resp_;
    }

    SnapshotMetadata snap;
    snap.set_last_included_index(req.last_included_index());
    snap.set_last_included_term(req.last_included_term());
    snap.set_metadata_store_data(req.snapshot_data());

    std::string serialized;
    snap.SerializeToString(&serialized);

    std::string tmp_path = SnapshotFilePath() + ".tmp";
    {
        std::ofstream file(tmp_path, std::ios::binary | std::ios::trunc);
        if (!file.is_open()) {
            snap_resp_.set_success(false);
            return &snap_resp_;
        }
        uint64_t len = serialized.size();
        file.write(reinterpret_cast<const char*>(&len), 8);
        file.write(serialized.data(), len);
        file.close();
    }
    fs::rename(tmp_path, SnapshotFilePath());

    if (restore_cb_ && !req.snapshot_data().empty())
        restore_cb_(req.snapshot_data());

    snapshot_last_index_ = req.last_included_index();
    snapshot_last_term_  = req.last_included_term();

    std::vector<LogEntry> new_log;
    new_log.push_back(log_[0]);
    for (size_t i = 1; i < log_.size(); i++)
        if (log_[i].index > snapshot_last_index_)
            new_log.push_back(log_[i]);
    log_.swap(new_log);

    if (commit_index_ < snapshot_last_index_) {
        commit_index_ = snapshot_last_index_;
        last_applied_ = snapshot_last_index_;
    }
    PersistState();
    snap_resp_.set_success(true);

    std::cout << "[Raft " << my_id_ << "] Installed snapshot from " << from
              << " at index " << req.last_included_index() << std::endl;
    return &snap_resp_;
}

// ===== Query =====

bool RaftNode::IsLeader() const {
    std::lock_guard<std::recursive_mutex> lock(state_mutex_);
    return state_ == RaftState::LEADER;
}

std::string RaftNode::GetLeaderID() const {
    std::lock_guard<std::recursive_mutex> lock(state_mutex_);
    return leader_id_;
}

RaftState RaftNode::GetState() const {
    std::lock_guard<std::recursive_mutex> lock(state_mutex_);
    return state_;
}

uint64_t RaftNode::GetCurrentTerm() const {
    std::lock_guard<std::recursive_mutex> lock(state_mutex_);
    return current_term_;
}

uint64_t RaftNode::GetCommitIndex() const {
    std::lock_guard<std::recursive_mutex> lock(state_mutex_);
    return commit_index_;
}

uint64_t RaftNode::GetLastLogIndex() const {
    std::lock_guard<std::recursive_mutex> lock(state_mutex_);
    return log_.back().index;
}

uint64_t RaftNode::GetLastLogTerm() const {
    std::lock_guard<std::recursive_mutex> lock(state_mutex_);
    return log_.back().term;
}

size_t RaftNode::GetLogSize() const {
    std::lock_guard<std::recursive_mutex> lock(state_mutex_);
    return log_.size() - 1;
}

// ===== Timer =====

void RaftNode::ResetElectionTimer() {
    std::lock_guard<std::mutex> lock(timer_mutex_);
    int timeout_ms = RandomElectionTimeout();
    election_deadline_ = std::chrono::steady_clock::now()
                         + std::chrono::milliseconds(timeout_ms);
}

void RaftNode::OnElectionTimeout() {
    RaftState s;
    {
        std::lock_guard<std::recursive_mutex> lock(state_mutex_);
        s = state_;
    }
    if (s == RaftState::LEADER) {
        SendHeartbeats();
        ResetElectionTimer();
    } else {
        BecomeCandidate();
        StartElection();
        ResetElectionTimer();
    }
}

void RaftNode::TimerLoop() {
    while (!stop_.load()) {
        bool should_fire = false;
        {
            std::lock_guard<std::mutex> lock(timer_mutex_);
            auto now = std::chrono::steady_clock::now();
            should_fire = (now >= election_deadline_);
        }
        if (should_fire) OnElectionTimeout();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

int RaftNode::RandomElectionTimeout() const {
    static thread_local std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<int> dist(
        config_.election_timeout_ms,
        config_.election_timeout_ms * 2);
    return dist(gen);
}

// ===== Log Helpers =====

LogEntry* RaftNode::GetLogEntry(uint64_t index) {
    for (auto& e : log_)
        if (e.index == index) return &e;
    return nullptr;
}

const LogEntry* RaftNode::GetLogEntry(uint64_t index) const {
    for (const auto& e : log_)
        if (e.index == index) return &e;
    return nullptr;
}

// ===== Persistence =====

std::string RaftNode::StateFilePath() const {
    return config_.data_dir + "/raft_state.dat";
}

std::string RaftNode::SnapshotFilePath() const {
    return config_.data_dir + "/raft_snapshot.dat";
}

bool RaftNode::PersistState() {
    std::ofstream file(StateFilePath(), std::ios::binary | std::ios::trunc);
    if (!file.is_open()) {
        std::cerr << "[Raft " << my_id_ << "] Failed to write state\n";
        return false;
    }
    file.write(reinterpret_cast<const char*>(&current_term_), 8);
    uint64_t vf_len = voted_for_.size();
    file.write(reinterpret_cast<const char*>(&vf_len), 8);
    if (vf_len > 0) file.write(voted_for_.data(), vf_len);
    uint64_t log_count = log_.size() - 1;
    file.write(reinterpret_cast<const char*>(&log_count), 8);
    for (size_t i = 1; i < log_.size(); i++) {
        file.write(reinterpret_cast<const char*>(&log_[i].term), 8);
        file.write(reinterpret_cast<const char*>(&log_[i].index), 8);
        uint64_t cmd_len = log_[i].command.size();
        file.write(reinterpret_cast<const char*>(&cmd_len), 8);
        if (cmd_len > 0) file.write(log_[i].command.data(), cmd_len);
    }
    file.close();
    return !file.fail();
}

bool RaftNode::RestoreState() {
    std::ifstream file(StateFilePath(), std::ios::binary);
    if (!file.is_open()) return true;

    file.read(reinterpret_cast<char*>(&current_term_), 8);
    if (file.gcount() != 8) return true;
    uint64_t vf_len = 0;
    file.read(reinterpret_cast<char*>(&vf_len), 8);
    if (file.gcount() == 8 && vf_len > 0) {
        voted_for_.resize(vf_len);
        file.read(&voted_for_[0], vf_len);
    }
    uint64_t log_count = 0;
    file.read(reinterpret_cast<char*>(&log_count), 8);
    if (file.gcount() != 8) return true;

    log_.clear();
    LogEntry d; d.term = 0; d.index = 0;
    log_.push_back(d);
    for (uint64_t i = 0; i < log_count; i++) {
        LogEntry entry;
        file.read(reinterpret_cast<char*>(&entry.term), 8);
        file.read(reinterpret_cast<char*>(&entry.index), 8);
        uint64_t cmd_len = 0;
        file.read(reinterpret_cast<char*>(&cmd_len), 8);
        if (cmd_len > 0) {
            entry.command.resize(cmd_len);
            file.read(&entry.command[0], cmd_len);
        }
        log_.push_back(entry);
    }
    commit_index_ = log_.back().index;
    last_applied_ = commit_index_;
    std::cout << "[Raft " << my_id_ << "] Restored term=" << current_term_
              << " log=" << (log_.size() - 1) << " commit=" << commit_index_ << std::endl;
    return true;
}

// ===== Snapshot =====

bool RaftNode::TakeSnapshot() {
    if (!snapshot_cb_) return false;
    std::string data;
    uint64_t last_included_index;
    uint64_t last_included_term;
    {
        std::lock_guard<std::recursive_mutex> lock(state_mutex_);
        if (last_applied_ <= snapshot_last_index_) return false;
        data = snapshot_cb_();
        last_included_index = last_applied_;
        const LogEntry* entry = GetLogEntry(last_included_index);
        last_included_term = entry ? entry->term : current_term_;
    }

    SnapshotMetadata snap;
    snap.set_last_included_index(last_included_index);
    snap.set_last_included_term(last_included_term);
    snap.set_metadata_store_data(data);

    std::string serialized;
    snap.SerializeToString(&serialized);

    std::string tmp = SnapshotFilePath() + ".tmp";
    {
        std::ofstream file(tmp, std::ios::binary | std::ios::trunc);
        if (!file.is_open()) return false;
        uint64_t len = serialized.size();
        file.write(reinterpret_cast<const char*>(&len), 8);
        file.write(serialized.data(), len);
    }
    fs::rename(tmp, SnapshotFilePath());

    {
        std::lock_guard<std::recursive_mutex> lock(state_mutex_);
        snapshot_last_index_ = last_included_index;
        snapshot_last_term_  = last_included_term;
        std::vector<LogEntry> new_log;
        new_log.push_back(log_[0]);
        for (size_t i = 1; i < log_.size(); i++)
            if (log_[i].index > last_included_index)
                new_log.push_back(log_[i]);
        log_.swap(new_log);
        PersistState();
    }
    std::cout << "[Raft " << my_id_ << "] Snapshot at index " << last_included_index << std::endl;
    return true;
}

bool RaftNode::RestoreFromSnapshot() {
    std::ifstream file(SnapshotFilePath(), std::ios::binary);
    if (!file.is_open()) return true;

    uint64_t len = 0;
    file.read(reinterpret_cast<char*>(&len), 8);
    if (file.gcount() != 8) return true;
    std::string s(len, '\0');
    file.read(&s[0], len);
    if ((uint64_t)file.gcount() != len) return true;

    SnapshotMetadata snap;
    if (!snap.ParseFromString(s)) {
        std::cerr << "[Raft " << my_id_ << "] Bad snapshot\n";
        return false;
    }
    if (restore_cb_ && !snap.metadata_store_data().empty())
        restore_cb_(snap.metadata_store_data());

    snapshot_last_index_ = snap.last_included_index();
    snapshot_last_term_  = snap.last_included_term();
    if (commit_index_ < snapshot_last_index_) {
        commit_index_ = snapshot_last_index_;
        last_applied_ = snapshot_last_index_;
    }
    std::cout << "[Raft " << my_id_ << "] Restored snapshot index="
              << snapshot_last_index_ << std::endl;
    return true;
}

}  // namespace mini_storage
