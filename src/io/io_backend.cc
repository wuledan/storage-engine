#include "io_backend.h"
#include "runtime/affinity_baton.h"

namespace storage::io {
using runtime::adapt::Task;

namespace {

// IOState lives on the coroutine frame — zero heap allocation
struct IOState {
    storage::runtime::adapt::AffinityBaton baton;
    IOCompletion result;
};

// Captureless completion handler (function pointer compatible)
void io_rw_complete(void* ctx, IOCompletion comp) {
    auto* state = static_cast<IOState*>(ctx);
    state->result = comp;
    state->baton.post();
}

}  // anonymous namespace

Task<IOCompletion> IIOBackend::co_read(
    int fd, uint64_t offset, void* buf, size_t len) {
    // All on coroutine frame — zero heap allocations
    IOState state;
    IORequest req;

    req.op = IORequest::kRead;
    req.fd = fd;
    req.offset = offset;
    req.buf = buf;
    req.len = len;
    req.callback_fn = io_rw_complete;
    req.callback_ctx = &state;

    try {
        this->submit(std::move(req));
    } catch (const std::exception& e) {
        state.result.result = -EIO;
        co_return state.result;
    }
    co_await state.baton;
    co_return state.result;
}

Task<IOCompletion> IIOBackend::co_write(
    int fd, uint64_t offset, const void* buf, size_t len) {
    IOState state;
    IORequest req;

    req.op = IORequest::kWrite;
    req.fd = fd;
    req.offset = offset;
    req.buf = const_cast<void*>(buf);
    req.len = len;
    req.callback_fn = io_rw_complete;
    req.callback_ctx = &state;

    try {
        this->submit(std::move(req));
    } catch (const std::exception& e) {
        state.result.result = -EIO;
        co_return state.result;
    }
    co_await state.baton;
    co_return state.result;
}

}  // namespace storage::io
