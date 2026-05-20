// main_namenode_ha.cpp
// Phase 3: High-Availability NameNode with Raft consensus
//
// Usage:
//   ./namenode_ha --id=1 --client_port=9000 --raft_port=9001
//
// Cluster peers are configured via command line:
//   --peers=127.0.0.1:9001,127.0.0.1:9101,127.0.0.1:9201
//
// The --id parameter selects which peer from the list is "me" (0-based).

#include "namenode/namenode_ha_server.h"
#include <iostream>
#include <cstring>
#include <vector>
#include <sstream>
#include <csignal>
#include <atomic>

using namespace mini_storage;

static std::atomic<bool> g_running{true};

static void SignalHandler(int) {
    g_running.store(false);
}

static std::vector<std::string> SplitPeers(const std::string& s) {
    std::vector<std::string> result;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, ',')) {
        if (!item.empty()) result.push_back(item);
    }
    return result;
}

static void PrintUsage(const char* prog) {
    std::cerr << "Usage: " << prog << " [options]\n"
              << "  --data_dir=PATH     Data directory (default: /tmp/nn_ha)\n"
              << "  --client_port=PORT  Client service port (default: 9000)\n"
              << "  --raft_port=PORT    Raft service port (default: 9001)\n"
              << "  --host=HOST         Bind host (default: 127.0.0.1)\n"
              << "  --peers=P1,P2,P3    Comma-separated peer list (raft addresses)\n"
              << "  --workers=N         Thread pool size (default: 4)\n"
              << "\n"
              << "Example (3-node cluster):\n"
              << "  Node 0: " << prog << " --client_port=9000 --raft_port=9001 "
              << "--peers=127.0.0.1:9001,127.0.0.1:9101,127.0.0.1:9201\n"
              << "  Node 1: " << prog << " --client_port=9100 --raft_port=9101 "
              << "--peers=127.0.0.1:9001,127.0.0.1:9101,127.0.0.1:9201\n"
              << "  Node 2: " << prog << " --client_port=9200 --raft_port=9201 "
              << "--peers=127.0.0.1:9001,127.0.0.1:9101,127.0.0.1:9201\n";
}

int main(int argc, char* argv[]) {
    signal(SIGINT, SignalHandler);
    signal(SIGTERM, SignalHandler);

    std::string data_dir    = "/tmp/nn_ha";
    std::string host        = "127.0.0.1";
    int         client_port = 9000;
    int         raft_port   = 9001;
    std::string peers_str;
    int         workers     = 4;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg.rfind("--data_dir=", 0) == 0)
            data_dir = arg.substr(11);
        else if (arg.rfind("--host=", 0) == 0)
            host = arg.substr(7);
        else if (arg.rfind("--client_port=", 0) == 0)
            client_port = std::stoi(arg.substr(14));
        else if (arg.rfind("--raft_port=", 0) == 0)
            raft_port = std::stoi(arg.substr(12));
        else if (arg.rfind("--peers=", 0) == 0)
            peers_str = arg.substr(8);
        else if (arg.rfind("--workers=", 0) == 0)
            workers = std::stoi(arg.substr(10));
        else if (arg == "--help" || arg == "-h") {
            PrintUsage(argv[0]);
            return 0;
        }
    }

    auto peers = SplitPeers(peers_str);
    if (peers.empty()) {
        std::cerr << "Error: --peers is required\n\n";
        PrintUsage(argv[0]);
        return 1;
    }

    // Verify my peer ID is in the list
    std::string my_peer = host + ":" + std::to_string(raft_port);
    bool found = false;
    for (const auto& p : peers) {
        if (p == my_peer) { found = true; break; }
    }
    if (!found) {
        std::cerr << "Error: my raft address " << my_peer
                  << " not found in --peers list\n";
        return 1;
    }

    std::cout << "=== Phase 3: HA NameNode ===\n"
              << "Data dir:    " << data_dir << "\n"
              << "Client:      " << host << ":" << client_port << "\n"
              << "Raft:        " << host << ":" << raft_port << "\n"
              << "Peers:       ";
    for (const auto& p : peers) std::cout << p << " ";
    std::cout << "\nWorkers:     " << workers << "\n"
              << "=============================\n\n";

    HANameNodeServer server(data_dir,
                             host, client_port,
                             host, raft_port,
                             peers, workers);

    if (!server.Start()) {
        std::cerr << "Failed to start HA NameNode\n";
        return 1;
    }

    std::cout << "HA NameNode running. Press Ctrl+C to stop.\n";

    while (g_running.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    std::cout << "\nShutting down...\n";
    server.Stop();
    std::cout << "Done.\n";

    return 0;
}
