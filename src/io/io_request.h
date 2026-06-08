#pragma once
#include <cstdint>
#include <cstddef>
#include <functional>

namespace storage::io {

struct IOCompletion;

struct IORequest {
    enum Op : uint8_t { kRead, kWrite, kFlush, kTrim };

    Op op{kRead};
    int fd{-1};
    uint64_t offset{0};
    void* buf{nullptr};
    size_t len{0};
    uint32_t trace_id{0};

    using Callback = std::function<void(IOCompletion)>;
    Callback callback;
};

struct IOCompletion {
    uint64_t user_data{0};
    int64_t result{0};  // 成功: bytes, 失败: -errno
    IORequest::Callback callback;
};

}  // namespace storage::io
