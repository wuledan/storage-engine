#include "libaio_backend.h"
#include <cstring>
#include <stdexcept>
#include <algorithm>

namespace storage::io {

LibaioBackend::LibaioBackend(size_t queue_depth, IIOBackend::RouteFn route)
    : queue_depth_(queue_depth) {
    set_route_fn(std::move(route));
    pending_.resize(queue_depth_ * 2);
    iocbs_.resize(1);  // 至少 1 个 iocb 槽

    int ret = io_setup(queue_depth_, &ctx_);
    if (ret < 0) {
        throw std::runtime_error("io_setup failed: " + std::string(strerror(-ret)));
    }
}

LibaioBackend::~LibaioBackend() {
    io_destroy(ctx_);
}

void LibaioBackend::submit(IORequest req) {
    uint64_t idx = submit_count_++;
    if (idx >= pending_.size()) {
        pending_.resize(idx + 1);
    }
    pending_[idx] = std::move(req);

    struct iocb* cb = &iocbs_[0];
    std::memset(cb, 0, sizeof(*cb));

    switch (pending_[idx].op) {
    case IORequest::kRead:
        io_prep_pread(cb, pending_[idx].fd,
                      pending_[idx].buf, pending_[idx].len,
                      pending_[idx].offset);
        break;
    case IORequest::kWrite:
        io_prep_pwrite(cb, pending_[idx].fd,
                       pending_[idx].buf, pending_[idx].len,
                       pending_[idx].offset);
        break;
    default:
        return;
    }

    cb->data = reinterpret_cast<void*>(idx);
    struct iocb* cbs[1] = {cb};
    io_submit(ctx_, 1, cbs);
}

size_t LibaioBackend::poll(IOCompletion* out, size_t max) {
    struct io_event events[64];
    size_t to_get = std::min(max, sizeof(events) / sizeof(events[0]));
    
    // timeout=0: 非阻塞
    struct timespec ts = {0, 0};
    int ret = io_getevents(ctx_, 0, to_get, events, &ts);
    if (ret <= 0) return 0;

    for (int i = 0; i < ret; ++i) {
        uint64_t idx = reinterpret_cast<uint64_t>(events[i].data);
        out[i].result = events[i].res;
        out[i].user_data = idx;
        if (idx < pending_.size()) {
            out[i].callback = std::move(pending_[idx].callback);
        }
    }

    return static_cast<size_t>(ret);
}

}  // namespace storage::io
