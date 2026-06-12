#pragma once
#include "metric_counter.h"
#include "metric_histogram.h"
#include "metric_window.h"
#include <string>
#include <thread>
#include <atomic>
#include <functional>
#include <unordered_map>
#include <mutex>

namespace storage::runtime {

// Simple HTTP metrics server.
// Listens on a port, serves /metrics (JSON + Prometheus) and /health.
// Single-threaded, blocking accept loop — run in a background std::thread.
class MetricServer {
public:
    struct Config {
        int port{9090};
        std::string bind_addr{"0.0.0.0"};
    };

    MetricServer();
    explicit MetricServer(const Config& cfg);
    ~MetricServer();

    MetricServer(const MetricServer&) = delete;
    MetricServer& operator=(const MetricServer&) = delete;

    void start();
    void stop();

    // Add custom endpoint
    void add_endpoint(const std::string& path,
                      std::function<std::string()> handler);

private:
    void serve();
    std::string handle_request(const std::string& req);

    Config cfg_;
    int listen_fd_{-1};
    std::thread thread_;
    std::atomic<bool> running_{false};
    std::unordered_map<std::string, std::function<std::string()>> endpoints_;
    std::mutex endpoints_mutex_;
};

}  // namespace storage::runtime
