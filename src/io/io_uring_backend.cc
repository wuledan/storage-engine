#include "io_uring_backend.h"
#include "runtime/storage_error.h"
#include "runtime/metric_counter.h"
#include "runtime/metric_histogram.h"
#include <liburing.h>
#include <cstring>
#include <stdexcept>
#include <cassert>

namespace storage::io {

namespace {
storage::runtime::metric::MetricCounter g_io_submitted;
storage::runtime::metric::MetricCounter g_io_completed;
storage::runtime::metric::MetricLatency g_io_latency;
}

std::unordered_map<int, int> IOUringBackend::group_ring_fds_;

IOUringBackend::IOUringBackend(const IOBackendConfig& cfg, IIOBackend::RouteFn route)
    : queue_depth_(cfg.queue_depth) {
    set_route_fn(std::move(route));
    pending_.reserve(queue_depth_ * 2);
    pending_.resize(queue_depth_ * 2);

    struct io_uring_params params;
    std::memset(&params, 0, sizeof(params));

    if (cfg.sq_poll_group >= 0) {
        // Shared group: find or create the primary ring
        auto it = group_ring_fds_.find(cfg.sq_poll_group);
        if (it != group_ring_fds_.end()) {
            // Secondary ring in this group: attach to the primary
            params.flags |= IORING_SETUP_ATTACH_WQ;
            params.wq_fd = it->second;
        } else {
            // This ring becomes the primary for this group
            params.flags |= IORING_SETUP_SQPOLL;
            params.flags |= IORING_SETUP_SQ_AFF;
            params.sq_thread_idle = 0;
            params.sq_thread_cpu = 2;
            group_ring_fds_[cfg.sq_poll_group] = -1;  // placeholder, set after init
        }
    } else {
        // Own kernel thread (N=M)
        params.flags |= IORING_SETUP_SQPOLL;
        params.flags |= IORING_SETUP_SQ_AFF;     // 需要 sq_thread_cpu 生效
        params.sq_thread_idle = 0;               // 永不 idle，紧密自旋
        params.sq_thread_cpu = 2;               // CPU2 — dedicated SQPOLL core
    }

    int ret = io_uring_queue_init_params(queue_depth_, &ring_, &params);
    if (ret < 0) {
        throw std::runtime_error(
            std::string("[io] IORingSetupFailed: io_uring_queue_init failed, ret=")
            + std::to_string(-ret) + " (" + strerror(-ret) + ")");
    }

    // If this ring is the primary for a group, store the actual ring fd
    if (cfg.sq_poll_group >= 0 && !(params.flags & IORING_SETUP_ATTACH_WQ)) {
        group_ring_fds_[cfg.sq_poll_group] = ring_.ring_fd;
    }
}

IOUringBackend::~IOUringBackend() {
    // If this ring was the primary for a group, remove the entry
    for (auto it = group_ring_fds_.begin(); it != group_ring_fds_.end(); ++it) {
        if (it->second == ring_.ring_fd) {
            group_ring_fds_.erase(it);
            break;
        }
    }
    io_uring_queue_exit(&ring_);
}

// 新 submit: 走缓冲
void IOUringBackend::submit(IORequest req) {
    submit_impl(std::move(req));  // 单请求直通，不走缓冲
}

void IOUringBackend::submit_batch(std::vector<IORequest> requests) {
    // 满足最小深度或未配置 → 直接提交
    if (requests.size() >= buf_cfg_.min_batch_depth) {
        for (auto& req : requests) submit_impl(std::move(req));
        flush_submissions();
        return;
    }

    // 不足最小深度 → 缓冲
    for (auto& req : requests)
        incoming_.push_back({std::move(req), 0});
}

// 实际填充 SQE（原来的 submit 逻辑）
void IOUringBackend::submit_impl(IORequest req) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
        io_uring_submit(&ring_);
        pending_sqe_count_ = 0;
        sqe = io_uring_get_sqe(&ring_);
        if (!sqe) {
            if (req.callback) {
                IOCompletion comp;
                comp.result = -ENOBUFS;
                comp.callback = std::move(req.callback);
                comp.callback(comp);
            }
            return;
        }
    }

    uint64_t idx = submit_count_++;
    if (idx >= pending_.size()) {
        pending_.resize(std::max(pending_.size() * 2, idx + 1));
    }
    pending_[idx] = std::move(req);
    g_io_submitted << 1;

    switch (pending_[idx].op) {
    case IORequest::kRead:
        io_uring_prep_read(sqe, pending_[idx].fd,
                           pending_[idx].buf, pending_[idx].len,
                           pending_[idx].offset);
        break;
    case IORequest::kWrite:
        io_uring_prep_write(sqe, pending_[idx].fd,
                             pending_[idx].buf, pending_[idx].len,
                             pending_[idx].offset);
        break;
    default:
        break;
    }
    io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(idx));
    pending_sqe_count_++;
}

void IOUringBackend::flush_pending() {
    if (buf_cfg_.min_batch_depth == 0) return;
    // 单线程访问，无需锁
    for (auto& e : incoming_) e.age++;
    bool go = (incoming_.size() >= buf_cfg_.min_batch_depth);
    if (!go) {
        for (auto& e : incoming_) {
            if (e.age >= buf_cfg_.max_age_iterations) { go = true; break; }
        }
    }
    if (!go) return;
    for (auto& e : incoming_) submit_impl(std::move(e.request));
    incoming_.clear();
    flush_submissions();
}

void IOUringBackend::flush_submissions() {
    if (pending_sqe_count_ > 0) {
        io_uring_submit(&ring_);
        pending_sqe_count_ = 0;
    }
}

int IOUringBackend::submit_and_wait(size_t wait_nr) {
    if (pending_sqe_count_ == 0) return 0;
    int ret = io_uring_submit_and_wait(&ring_, wait_nr);
    pending_sqe_count_ = 0;
    needs_cqe_flush_ = false;  // submit_and_wait 内部已处理 task_work
    return ret;
}

void IOUringBackend::flush_cqe_task_work() {
    io_uring_enter(ring_.enter_ring_fd, 0, 0, IORING_ENTER_GETEVENTS, nullptr);
}

size_t IOUringBackend::poll(IOCompletion* out, size_t max) {
    size_t count = 0;
    struct io_uring_cqe* cqe = nullptr;

    while (count < max) {
        int ret = io_uring_peek_cqe(&ring_, &cqe);
        if (ret == -EAGAIN) break;
        if (ret < 0) {
            // 轮询错误，填入首个输出槽
            out[count].result = ret;
            out[count].user_data = 0;
            count++;
            break;
        }

        uint64_t idx = reinterpret_cast<uint64_t>(io_uring_cqe_get_data(cqe));
        assert(idx < pending_.size());

        out[count].result = cqe->res;
        out[count].user_data = idx;
        out[count].callback = std::move(pending_[idx].callback);

        io_uring_cqe_seen(&ring_, cqe);
        g_io_completed << 1;
        count++;
    }

    return count;
}

void register_io_metrics() {
    using namespace storage::runtime::metric;
    MetricRegistry::instance().register_counter("io/submitted", &g_io_submitted);
    MetricRegistry::instance().register_counter("io/completed", &g_io_completed);
    g_io_latency.register_with("io/latency");
}

}  // namespace storage::io
