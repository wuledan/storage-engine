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

IOUringBackend::IOUringBackend(const IOBackendConfig& cfg)
    : queue_depth_(cfg.queue_depth) {
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
            group_ring_fds_[cfg.sq_poll_group] = -1;  // placeholder, set after init
        }
    }

    // Rings that are NOT secondary rings in a shared group need their own
    // SQPOLL kernel thread + performance flags. Secondary rings attach to
    // the primary's workqueue and don't need their own thread.
    if (cfg.sq_poll_group < 0 || !(params.flags & IORING_SETUP_ATTACH_WQ)) {
        params.flags |= IORING_SETUP_SQPOLL;
        params.flags |= IORING_SETUP_SQ_AFF;
        params.flags |= IORING_SETUP_SINGLE_ISSUER;   // reduce kernel worker threads
        params.sq_thread_idle = 0;
        params.sq_thread_cpu = 2;
    }

    int ret = io_uring_queue_init_params(queue_depth_, &ring_, &params);
    if (ret < 0) {
        throw std::runtime_error(
            std::string("[io] IORingSetupFailed: io_uring_queue_init failed, ret=")
            + std::to_string(-ret) + " (" + strerror(-ret) + ")");
    }

    // If this ring is the primary for a group, store the ring fd so secondary
    // rings can attach to this ring's workqueue via IORING_SETUP_ATTACH_WQ.
    // NOTE: on newer kernels params.wq_fd would carry the io-wq fd, but on
    // kernel 6.17 the kernel accepts ring_fd directly for ATTACH_WQ.
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
    submit_impl(std::move(req));
    flush_submissions();  // 立即提交 SQE 到内核，避免 IO 请求永久挂起
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
        // SQ 环形缓冲区已满：提交所有待处理的 SQE，然后等待 CQE
        // 以释放 SQ 槽位。
        //
        // 注意：在 SQPOLL 模式下，io_uring_submit() 异步唤醒内核线程，
        // io_uring_wait_cqe() 返回时 khead 不一定已同步推进，
        // 因此需要循环等待处理 CQE，直到至少一个 SQ 槽位可用。
        io_uring_submit(&ring_);
        pending_sqe_count_ = 0;

        // 最多等待 ring_size*2 个 CQE：即使 SQPOLL 线程处理滞后，
        // 每个 CQE 必然对应一个被消费的 SQE，最终一定能释放槽位。
        for (int drain_attempts = 0; drain_attempts < (int)(queue_depth_ * 2); ++drain_attempts) {
            struct io_uring_cqe* cqe = nullptr;
            int ret = io_uring_wait_cqe(&ring_, &cqe);
            if (ret == 0 && cqe) {
                uint64_t idx = reinterpret_cast<uint64_t>(io_uring_cqe_get_data(cqe));
                if (idx < pending_.size()) {
                    IOCompletion comp;
                    comp.result = cqe->res;
                    comp.user_data = idx;
                    if (pending_[idx].callback_fn) {
                        pending_[idx].callback_fn(pending_[idx].callback_ctx, comp);
                    }
                    io_uring_cqe_seen(&ring_, cqe);
                } else {
                    io_uring_cqe_seen(&ring_, cqe);
                }
            }

            sqe = io_uring_get_sqe(&ring_);
            if (sqe)
                goto got_sqe;
        }

        // 耗尽重试次数仍无法获取 SQE → 投递业务错误
        if (req.callback_fn) {
            IOCompletion comp;
            comp.result = -ENOBUFS;
            req.callback_fn(req.callback_ctx, comp);
        }
        return;
    }

got_sqe:

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

        // Invoke callback inline (zero allocation on coroutine frame)
        if (pending_[idx].callback_fn) {
            pending_[idx].callback_fn(pending_[idx].callback_ctx, out[count]);
        }

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
