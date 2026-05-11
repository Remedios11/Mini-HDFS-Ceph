#include "namenode_edit_log.h"
#include <arpa/inet.h>
#include <fstream>
#include <iostream>
#include <stdexcept>

namespace mini_storage {

EditLog::EditLog(const std::string& log_path) : log_path_(log_path) {
    write_file_.open(log_path, std::ios::binary | std::ios::app);
    if (!write_file_.is_open())
        throw std::runtime_error("Cannot open edit log: " + log_path);
}

EditLog::~EditLog() { Close(); }

bool EditLog::Append(const NameNodeRequest& op) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::string bytes;
    if (!op.SerializeToString(&bytes)) return false;
    uint32_t len = htonl((uint32_t)bytes.size());
    write_file_.write(reinterpret_cast<const char*>(&len), 4);
    write_file_.write(bytes.data(), bytes.size());
    write_file_.flush();
    return !write_file_.fail();
}

bool EditLog::Replay(ReplayCallback callback) {
    std::ifstream file(log_path_, std::ios::binary);
    if (!file.is_open()) return true;  // no history

    int count = 0;
    while (file.good()) {
        uint32_t len;
        file.read(reinterpret_cast<char*>(&len), 4);
        if (file.gcount() == 0) break;
        if (file.gcount() != 4) { std::cerr << "[EditLog] Truncated\n"; break; }

        len = ntohl(len);
        std::string bytes(len, '\0');
        file.read(&bytes[0], len);
        if ((uint32_t)file.gcount() != len) { std::cerr << "[EditLog] Incomplete\n"; break; }

        NameNodeRequest op;
        if (op.ParseFromString(bytes)) { callback(op); count++; }
    }
    std::cout << "[EditLog] Replayed " << count << " entries\n";
    return true;
}

void EditLog::Close() {
    if (write_file_.is_open()) write_file_.close();
}

}  // namespace mini_storage
