#include "libaio_backend.h"
#include "runtime/storage_error.h"
#include <cstring>
#include <stdexcept>
#include <algorithm>

namespace storage::io {

LibaioBackend::LibaioBackend(size_t queue_depth)
    : queue_depth_(queue_depth) {
    pending_.resize(queue_depth_ * 2);
    iocbs_.resize(1);

    int ret = io_setup(queue_depth_, &ctx_);
    if (ret < 0) {
        throw std::runtime_error(
            std::string("[io] IOAioSetupFailed: io_setup failed, ret=")
            + std::to_string(-ret) + " (" + strerror(-ret) + ")");
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
    int ret = io_submit(ctx_, 1, cbs);
    if (ret < 0) {
        // 提交失败，通过 callback 通知
        if (pending_[idx].callback_fn) {
            IOCompletion comp;
            comp.result = ret;
            comp.user_data = idx;
            pending_[idx].callback_fn(pending_[idx].callback_ctx, comp);
        }
    }
}

size_t LibaioBackend::poll(IOCompletion* out, size_t max) {
    struct io_event events[64];
    size_t to_get = std::min(max, sizeof(events) / sizeof(events[0]));

    struct timespec ts = {0, 0};
    int ret = io_getevents(ctx_, 0, static_cast<long>(to_get), events, &ts);
    if (ret < 0) {
        // io_getevents 错误，填入首个输出槽
        out[0].result = ret;
        out[0].user_data = 0;
        return 1;
    }
    if (ret == 0) return 0;

    for (int i = 0; i < ret; ++i) {
        uint64_t idx = reinterpret_cast<uint64_t>(events[i].data);
        out[i].result = events[i].res;
        out[i].user_data = idx;
        if (idx < pending_.size() && pending_[idx].callback_fn) {
            pending_[idx].callback_fn(pending_[idx].callback_ctx, out[i]);
        }
    }

    return static_cast<size_t>(ret);
}

}  // namespace storage::io
