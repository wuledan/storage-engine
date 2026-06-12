#include "metric_server.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <sstream>

namespace storage::runtime {

MetricServer::MetricServer() : MetricServer(Config{}) {}

MetricServer::MetricServer(const Config& cfg) : cfg_(cfg) {
    // Register default endpoints
    add_endpoint("/metrics", []() {
        return metric::MetricRegistry::instance().to_json();
    });

    add_endpoint("/metrics?format=prometheus", []() {
        return metric::MetricRegistry::instance().to_prometheus();
    });

    add_endpoint("/health", []() { return "{\"status\":\"ok\"}"; });
}

MetricServer::~MetricServer() { stop(); }

void MetricServer::add_endpoint(const std::string& path, std::function<std::string()> h) {
    std::lock_guard<std::mutex> lk(endpoints_mutex_);
    endpoints_[path] = std::move(h);
}

void MetricServer::start() {
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) return;

    int opt = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(cfg_.port);
    addr.sin_addr.s_addr = INADDR_ANY;

    bind(listen_fd_, (sockaddr*)&addr, sizeof(addr));
    listen(listen_fd_, 5);

    running_.store(true);
    thread_ = std::thread(&MetricServer::serve, this);
}

void MetricServer::stop() {
    running_.store(false);
    if (listen_fd_ >= 0) {
        shutdown(listen_fd_, SHUT_RDWR);
        close(listen_fd_);
        listen_fd_ = -1;
    }
    if (thread_.joinable()) thread_.join();
}

void MetricServer::serve() {
    char buf[8192];
    while (running_.load(std::memory_order_acquire)) {
        int fd = accept(listen_fd_, nullptr, nullptr);
        if (fd < 0) continue;

        ssize_t n = recv(fd, buf, sizeof(buf) - 1, 0);
        if (n > 0) {
            buf[n] = 0;
            std::string response = handle_request(buf);
            send(fd, response.c_str(), response.size(), 0);
        }
        close(fd);
    }
}

std::string MetricServer::handle_request(const std::string& req) {
    // Parse GET /path HTTP/1.1
    std::string path = "/";
    size_t p0 = req.find("GET /");
    if (p0 != std::string::npos) {
        size_t p1 = req.find(" HTTP", p0);
        if (p1 != std::string::npos) {
            path = req.substr(p0 + 4, p1 - p0 - 4);
        }
    }

    std::string body;
    std::string content_type = "application/json";

    std::lock_guard<std::mutex> lk(endpoints_mutex_);
    // Try exact match first, then prefix match for query strings
    auto it = endpoints_.find(path);
    if (it == endpoints_.end()) {
        // Try without query string
        size_t q = path.find('?');
        if (q != std::string::npos) {
            it = endpoints_.find(path.substr(0, q));
        }
    }

    if (it != endpoints_.end()) {
        body = it->second();
    } else {
        body = "{\"error\":\"not found\",\"path\":\"" + path + "\"}";
    }

    // CORS header for dashboard
    std::ostringstream oss;
    oss << "HTTP/1.1 200 OK\r\n"
        << "Content-Type: " << content_type << "\r\n"
        << "Access-Control-Allow-Origin: *\r\n"
        << "Content-Length: " << body.size() << "\r\n"
        << "\r\n"
        << body;
    return oss.str();
}

}  // namespace storage::runtime
