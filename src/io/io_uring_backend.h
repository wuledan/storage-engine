#pragma once
#include "io_engine.h"
#include "io_backend.h"
#include "runtime/types.h"
#include <liburing.h>
#include <array>
#include <vector>
#include <unordered_map>
#include <cstring>

namespace storage::io {

class IOUringBackend : public IIOBackend {
public:
    struct BufferConfig {
        size_t min_batch_depth{0};         // 0=直通, >0=缓冲门槛
        uint64_t max_age_iterations{3};    // 最大等待轮数
    };

    explicit IOUringBackend(const IOBackendConfig& cfg);
    ~IOUringBackend() override;

    std::string_view name() const noexcept override { return "io_uring"; }

    void submit(IORequest req) override;                      // 走缓冲
    void submit_batch(std::vector<IORequest> requests) override;  // 批量缓冲
    void flush_pending() override;                            // 决策+批量提交
    size_t pending_count() const noexcept override { return incoming_.size() + flushing_.size(); }
    void flush_submissions();  // 批量提交所有待处理 SQE
    int submit_and_wait(size_t wait_nr);  // fio-aligned: submit + kernel wait for wait_nr CQEs
    struct io_uring* raw_ring() { return &ring_; }  // for low-level bench
    bool has_sqe_submitted() const noexcept override { return needs_cqe_flush_; }
    void flush_cqe_task_work() override;  // io_uring_enter to trigger kernel task_work for SINGLE_ISSUER
    size_t poll(IOCompletion* out, size_t max) override;

    void set_buffer_config(const BufferConfig& cfg) { buf_cfg_ = cfg; }

private:
    static std::unordered_map<int, int> group_ring_fds_;  // group_id → primary ring fd
    // 实际填充 SQE（内部方法，不缓冲）
    void submit_impl(IORequest req);

    struct BufferEntry {
        IORequest request;
        uint64_t age{0};
    };

    size_t queue_depth_;
    struct io_uring ring_;
    std::vector<IORequest> pending_;
    std::vector<BufferEntry> incoming_;   // 入队侧
    std::vector<BufferEntry> flushing_;   // 出队侧
    BufferConfig buf_cfg_;
    size_t submit_count_{0};
    size_t last_buffer_size_{0};
    size_t pending_sqe_count_{0};          // flush_submissions 用
    bool needs_cqe_flush_{false};           // SINGLE_ISSUER: need io_uring_enter to see CQEs
};

}  // namespace storage::io
