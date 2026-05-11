#include "net_thread_pool.h"

namespace mini_storage {

ThreadPool::ThreadPool(int num_threads) {
    for (int i = 0; i < num_threads; i++) {
        workers_.emplace_back([this] { WorkerLoop(); });
    }
}

ThreadPool::~ThreadPool() {
    Stop();
}

void ThreadPool::Submit(Task task) {
    {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        task_queue_.push(std::move(task));
    }
    cv_.notify_one();
}

void ThreadPool::WorkerLoop() {
    while (true) {
        Task task;
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            cv_.wait(lock, [this] {
                return !task_queue_.empty() || stop_.load();
            });
            if (stop_.load() && task_queue_.empty()) return;
            task = std::move(task_queue_.front());
            task_queue_.pop();
        }
        task();
    }
}

void ThreadPool::Stop() {
    stop_.store(true);
    cv_.notify_all();
    for (auto& t : workers_) {
        if (t.joinable()) t.join();
    }
}

size_t ThreadPool::QueueSize() const {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    return task_queue_.size();
}

}  // namespace mini_storage
