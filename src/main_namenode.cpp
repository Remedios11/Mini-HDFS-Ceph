// main_namenode.cpp
// Usage: ./namenode_server [port] [data_dir]
#include "namenode_namenode_server.h"
#include <iostream>
#include <csignal>

static mini_storage::NameNodeServer* g_server = nullptr;

void signal_handler(int) {
    if (g_server) g_server->Stop();
}

int main(int argc, char* argv[]) {
    int         port     = (argc > 1) ? atoi(argv[1]) : 9090;
    std::string data_dir = (argc > 2) ? argv[2] : "./nn_data";

    std::signal(SIGINT,  signal_handler);
    std::signal(SIGTERM, signal_handler);

    mini_storage::NameNodeServer server(data_dir, "0.0.0.0", port, 4);
    g_server = &server;

    if (!server.Start()) {
        std::cerr << "Failed to start NameNode\n";
        return 1;
    }
    std::cout << "NameNode started on :" << port
              << "  data_dir=" << data_dir << "\n";
    server.Run();  // blocks
    return 0;
}
