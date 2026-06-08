#include <gtest/gtest.h>
#include <set>
#include <atomic>
#include <thread>
#include <chrono>
#include "runtime/storage_error.h"
#include "io/io_engine.h"

using namespace storage;
using namespace storage::io;

// ============================================================================
// 测试1: 模块名映射
// ============================================================================
TEST(ErrorCodesTest, ModuleNameMapping) {
    EXPECT_STREQ(module_name(StorageError::Unknown), "generic");
    EXPECT_STREQ(module_name(StorageError::WorkerStartFailed), "runtime");
    EXPECT_STREQ(module_name(StorageError::IORingSetupFailed), "io");
}

// ============================================================================
// 测试2: make_error_code
// ============================================================================
TEST(ErrorCodesTest, MakeErrorCode) {
    auto ec = make_error_code(StorageError::IORingSetupFailed);
    EXPECT_EQ(ec.value(), static_cast<int>(StorageError::IORingSetupFailed));
    EXPECT_STREQ(ec.category().name(), "storage-engine");
}

// ============================================================================
// 测试3: 编码唯一性
// ============================================================================
TEST(ErrorCodesTest, UniqueCodes) {
    std::set<uint32_t> seen;
    for (int i = 0; i <= 50; ++i) {
        auto c = static_cast<uint32_t>(static_cast<StorageError>(i));
        if (c != 0) {
            EXPECT_TRUE(seen.insert(c).second) << "Duplicate code: 0x" << std::hex << c;
        }
    }
}

// ============================================================================
// 测试4: IOBackend 通过 factory 创建
// ============================================================================
TEST(ErrorCodesTest, IOBackendAvailable) {
    auto b = IOEngine::create({"io_uring", 64}, nullptr);
    ASSERT_NE(b, nullptr);
    EXPECT_EQ(b->name(), "io_uring");
}

// ============================================================================
// 测试5: 无效 fd 的 IO 提交不导致 crash
// ============================================================================
TEST(ErrorCodesTest, InvalidFDHandled) {
    auto b = IOEngine::create({"io_uring", 64}, nullptr);
    ASSERT_NE(b, nullptr);

    IORequest req;
    req.op = IORequest::kRead;
    req.fd = -1;
    req.buf = nullptr;
    req.len = 0;

    std::atomic<bool> done{false};
    req.callback = [&done](IOCompletion) { done.store(true); };
    b->submit(std::move(req));

    IOCompletion out[64];
    for (int i = 0; i < 100 && !done.load(); ++i) {
        size_t n = b->poll(out, 64);
        for (size_t j = 0; j < n; ++j) {
            if (out[j].callback) out[j].callback(out[j]);
        }
        if (n == 0) std::this_thread::sleep_for(std::chrono::microseconds(100));
    }

    EXPECT_TRUE(done.load());
}
