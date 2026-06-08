#pragma once
#include <coroutine>
#include <cstdint>

namespace storage::runtime {

struct WorkItem {
    using Func = void(*)();

    union {
        Func func;
        std::coroutine_handle<> coro;
    };
    uint8_t tag;          // 0 = func, 1 = coroutine
    uint32_t trace_id{0};

    WorkItem() noexcept : func(nullptr), tag(0) {}

    void execute() noexcept {
        if (tag == 0) {
            func();
        } else {
            coro.resume();
        }
    }

    static WorkItem make_func(Func f) noexcept {
        WorkItem item;
        item.func = f;
        item.tag = 0;
        return item;
    }

    static WorkItem make_coro(std::coroutine_handle<> h) noexcept {
        WorkItem item;
        item.coro = h;
        item.tag = 1;
        return item;
    }

    // 供 ObjectPool 复用前重置
    void reset() noexcept {
        func = nullptr;
        tag = 0;
        trace_id = 0;
    }
};

}  // namespace storage::runtime
