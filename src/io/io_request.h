#pragma once
#include <cstdint>
#include <cstddef>

namespace storage::io {

struct IOCompletion {
    uint64_t user_data{0};
    int64_t result{0};  // 成功: bytes, 失败: -errno
};

struct IORequest {
    enum Op : uint8_t { kRead, kWrite, kFlush, kTrim };

    Op op{kRead};
    int fd{-1};
    uint64_t offset{0};
    void* buf{nullptr};
    size_t len{0};
    uint32_t trace_id{0};

    // Callback: function pointer + context (16 bytes, zero heap allocation)
    using CallbackFn = void(*)(void* ctx, IOCompletion comp);
    CallbackFn callback_fn = nullptr;
    void* callback_ctx = nullptr;

    // Convenience: invoke if callback is set
    void invoke_callback(IOCompletion comp) {
        if (callback_fn) callback_fn(callback_ctx, comp);
    }
};

}  // namespace storage::io
