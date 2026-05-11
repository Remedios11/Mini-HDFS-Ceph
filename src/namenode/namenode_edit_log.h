#pragma once
#include "proto/namenode.pb.h"
#include <fstream>
#include <string>
#include <mutex>
#include <functional>

namespace mini_storage {

class EditLog {
public:
    explicit EditLog(const std::string& log_path);
    ~EditLog();

    bool Append(const NameNodeRequest& op);

    using ReplayCallback = std::function<void(const NameNodeRequest&)>;
    bool Replay(ReplayCallback callback);

    void Close();

private:
    std::string   log_path_;
    std::ofstream write_file_;
    std::mutex    mutex_;
};

}  // namespace mini_storage
