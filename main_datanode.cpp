// main_datanode.cpp
// Usage: ./datanode_server <port> [data_dir] [namenode_host] [namenode_port]
#include "datanode_datanode_server.h"
#include <iostream>
#include <csignal>
#include <chrono>
#include <thread>

static mini_storage::DataNodeServer* g_server = nullptr;

void signal_handler(int) {
    if (g_server) g_server->Stop();
}

int main(int argc, char* argv[]) {
    int         port     = (argc > 1) ? atoi(argv[1])   : 9001;
    std::string data_dir = (argc > 2) ? argv[2]          : ("./dn_data_" + std::to_string(port));
    std::string nn_host  = (argc > 3) ? argv[3]          : "127.0.0.1";
    int         nn_port  = (argc > 4) ? atoi(argv[4])    : 9090;

    std::signal(SIGINT,  signal_handler);
    std::signal(SIGTERM, signal_handler);

    mini_storage::DataNodeServer server(data_dir, "0.0.0.0", port, nn_host, nn_port);
    g_server = &server;

    if (!server.Start()) {
        std::cerr << "Failed to start DataNode\n";
        return 1;
    }
    std::cout << "DataNode started on :" << port
              << "  NameNode=" << nn_host << ":" << nn_port
              << "  data_dir=" << data_dir << "\n";

    // Block until signalled
    while (g_server) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    return 0;
}
