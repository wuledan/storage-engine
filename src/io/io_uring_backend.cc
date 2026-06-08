#include "io_uring_backend.h"
#include "runtime/storage_error.h"
#include <liburing.h>
#include <cstring>
#include <stdexcept>
#include <cassert>

namespace storage::io {

IOUringBackend::IOUringBackend(size_t queue_depth, IIOBackend::RouteFn route)
    : queue_depth_(queue_depth) {
    set_route_fn(std::move(route));
    pending_.reserve(queue_depth_ * 2);
    pending_.resize(queue_depth_ * 2);

    struct io_uring_params params;
    std::memset(&params, 0, sizeof(params));
    int ret = io_uring_queue_init_params(queue_depth_, &ring_, &params);
    if (ret < 0) {
        throw std::runtime_error(
            std::string("[io] IORingSetupFailed: io_uring_queue_init failed, ret=")
            + std::to_string(-ret) + " (" + strerror(-ret) + ")");
    }
}

IOUringBackend::~IOUringBackend() {
    io_uring_queue_exit(&ring_);
}

// 新 submit: 走缓冲
void IOUringBackend::submit(IORequest req) {
    submit_impl(std::move(req));  // 单请求直通，不走缓冲
}

void IOUringBackend::submit_batch(std::vector<IORequest> requests) {
    std::lock_guard<std::mutex> lock(buffer_mutex_);
    for (auto& req : requests) {
        incoming_.push_back({std::move(req), 0, storage::runtime::Priority::kMedium});
    }
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
    // 双缓冲交换：短暂持锁将 incoming 交换到 flushing，释放锁后处理
    {
        std::lock_guard<std::mutex> lock(buffer_mutex_);
        if (incoming_.empty()) {
            last_buffer_size_ = 0;
            return;
        }
        flushing_.swap(incoming_);
    }

    // 老化
    for (auto& entry : flushing_) {
        entry.age++;
    }

    // 决策
    bool should_flush = false;

    if (flushing_.size() == 1 && flushing_.size() <= last_buffer_size_) {
        should_flush = true;
    } else if (flushing_.size() >= buf_cfg_.max_batch_size) {
        should_flush = true;
    } else {
        for (auto& entry : flushing_) {
            double effective = static_cast<double>(static_cast<uint8_t>(entry.priority))
                             - buf_cfg_.aging_weight * entry.age;
            if (effective <= 0 || entry.age >= buf_cfg_.max_age_iterations) {
                should_flush = true;
                break;
            }
        }
    }

    if (!should_flush) {
        // 不提交：flushing 中的请求回到 incoming（持锁）
        std::lock_guard<std::mutex> lock(buffer_mutex_);
        incoming_.insert(incoming_.end(),
                         std::make_move_iterator(flushing_.begin()),
                         std::make_move_iterator(flushing_.end()));
        flushing_.clear();
        last_buffer_size_ = incoming_.size();
        return;
    }

    // Flush
    for (auto& entry : flushing_) {
        submit_impl(std::move(entry.request));
    }
    last_buffer_size_ = flushing_.size();
    flushing_.clear();
    flush_submissions();
}

void IOUringBackend::flush_submissions() {
    if (pending_sqe_count_ > 0) {
        io_uring_submit(&ring_);
        pending_sqe_count_ = 0;
    }
}

size_t IOUringBackend::poll(IOCompletion* out, size_t max) {
    // Auto-flush: 确保缓冲的请求在 poll 前已提交
    flush_pending();
    flush_submissions();
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
        count++;
    }

    return count;
}

}  // namespace storage::io
