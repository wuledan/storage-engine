#include "io_backend.h"
#include "runtime/adapt/affinity_baton.h"
#include <memory>

namespace storage::io {

folly::coro::Task<IOCompletion> IIOBackend::co_read(
    int fd, uint64_t offset, void* buf, size_t len) {
    IORequest req;
    req.op = IORequest::kRead;
    req.fd = fd;
    req.offset = offset;
    req.buf = buf;
    req.len = len;

    auto baton = std::make_shared<storage::runtime::adapt::AffinityBaton>();
    IOCompletion result;

    req.callback = [baton, &result, route = route_fn_](IOCompletion comp) {
        result = comp;
        if (route) {
            baton->post([route](size_t wid, std::coroutine_handle<> h) {
                route(wid, h);
            });
        } else {
            baton->post_direct();
        }
    };

    try {
        this->submit(std::move(req));
    } catch (const std::exception& e) {
        result.result = -EIO;
        co_return result;
    }
    co_await *baton;
    co_return result;
}

folly::coro::Task<IOCompletion> IIOBackend::co_write(
    int fd, uint64_t offset, const void* buf, size_t len) {
    IORequest req;
    req.op = IORequest::kWrite;
    req.fd = fd;
    req.offset = offset;
    req.buf = const_cast<void*>(buf);
    req.len = len;

    auto baton = std::make_shared<storage::runtime::adapt::AffinityBaton>();
    IOCompletion result;

    req.callback = [baton, &result, route = route_fn_](IOCompletion comp) {
        result = comp;
        if (route) {
            baton->post([route](size_t wid, std::coroutine_handle<> h) {
                route(wid, h);
            });
        } else {
            baton->post_direct();
        }
    };

    try {
        this->submit(std::move(req));
    } catch (const std::exception& e) {
        result.result = -EIO;
        co_return result;
    }
    co_await *baton;
    co_return result;
}

}  // namespace storage::io
