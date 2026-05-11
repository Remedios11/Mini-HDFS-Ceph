// test/thread_pool_test.cpp
#include "net_thread_pool.h"
#include <iostream>
#include <atomic>
#include <cassert>
#include <chrono>
#include <thread>

int main() {
    // Test 1: Basic submit and execute
    {
        mini_storage::ThreadPool pool(4);
        std::atomic<int> counter{0};
        const int N = 1000;

        for (int i = 0; i < N; i++) {
            pool.Submit([&counter] { counter.fetch_add(1); });
        }

        // Wait for tasks to complete
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        pool.Stop();

        assert(counter.load() == N);
        std::cout << "✓ Test 1: Executed " << counter << " tasks\n";
    }

    // Test 2: Tasks execute concurrently
    {
        mini_storage::ThreadPool pool(4);
        std::atomic<int> max_concurrent{0};
        std::atomic<int> current{0};
        std::mutex m;

        for (int i = 0; i < 8; i++) {
            pool.Submit([&] {
                int val = current.fetch_add(1) + 1;
                int expected = max_concurrent.load();
                while (val > expected && !max_concurrent.compare_exchange_weak(expected, val))
                    expected = max_concurrent.load();
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                current.fetch_sub(1);
            });
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        pool.Stop();
        std::cout << "✓ Test 2: Max concurrent = " << max_concurrent << "\n";
        assert(max_concurrent > 1);
    }

    std::cout << "\n✅ ThreadPool all tests passed!\n";
    return 0;
}
