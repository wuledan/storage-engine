#include "io_uring_backend.h"
#include <liburing.h>
#include <cstring>
#include <stdexcept>
#include <cassert>

namespace storage::io {

IOUringBackend::IOUringBackend(size_t queue_depth, IIOBackend::RouteFn route)
    : queue_depth_(queue_depth) {
    set_route_fn(std::move(route));
    pending_.reserve(queue_depth_ * 2);
    // Resize to initial size
    pending_.resize(queue_depth_ * 2);

    struct io_uring_params params;
    std::memset(&params, 0, sizeof(params));
    int ret = io_uring_queue_init_params(queue_depth_, &ring_, &params);
    if (ret < 0) {
        throw std::runtime_error("io_uring_queue_init failed: " +
                                 std::string(strerror(-ret)));
    }
}

IOUringBackend::~IOUringBackend() {
    io_uring_queue_exit(&ring_);
}

void IOUringBackend::submit(IORequest req) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
        io_uring_submit(&ring_);
        sqe = io_uring_get_sqe(&ring_);
        if (!sqe) return;
    }

    // Use ever-increasing index to avoid overwriting in-flight callbacks
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
    io_uring_submit(&ring_);
}

size_t IOUringBackend::poll(IOCompletion* out, size_t max) {
    size_t count = 0;
    struct io_uring_cqe* cqe = nullptr;

    while (count < max) {
        int ret = io_uring_peek_cqe(&ring_, &cqe);
        if (ret == -EAGAIN) break;
        if (ret < 0) break;

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
