#pragma once
#include <cstdint>
#include <string>
#include <system_error>

namespace storage {

enum class StorageError : uint32_t {
    OK = 0,

    // Generic (0x0001xxxx)
    Unknown             = 0x00010001,
    InvalidArgument     = 0x00010002,
    Timeout             = 0x00010003,
    OutOfResource       = 0x00010004,
    NotInitialized      = 0x00010005,
    Cancelled           = 0x00010006,
    InternalError       = 0x00010007,
    NotImplemented      = 0x00010008,

    // Runtime (0x0010xxxx)
    WorkerStartFailed    = 0x00100001,
    WorkerAlreadyRunning = 0x00100002,
    WorkerStopTimeout    = 0x00100003,
    QueueFull            = 0x00100004,
    QueueOverflow        = 0x00100005,
    SchedulerStopped     = 0x00100006,
    PolicyInvalidName    = 0x00100007,
    MemoryPoolExhausted  = 0x00100008,
    CPUBindFailed        = 0x00100009,

    // IO (0x0020xxxx)
    IORingSetupFailed    = 0x00200001,
    IORingSubmitFailed   = 0x00200002,
    IOQueueFull          = 0x00200003,
    IOInvalidFD          = 0x00200004,
    IOBackendNotAvail    = 0x00200005,
    IOResultError        = 0x00200006,
    IOAioSetupFailed     = 0x00200007,
    IOAioSubmitFailed    = 0x00200008,
    IOAioGetEventsFail   = 0x00200009,
    IOSpdkNotReady       = 0x0020000A,
    IOPollError          = 0x0020000B,

    // RPC (0x0030xxxx)
    RPCConnectionRefused  = 0x00300001,
    RPCConnectionTimeout  = 0x00300002,
    RPCProtocolError      = 0x00300003,

    // Engine (0x0040xxxx)
    EngineNotSupported    = 0x00400001,
    EngineCorruption      = 0x00400002,
};

// 错误码 → 模块名映射
const char* module_name(StorageError e) noexcept;

// StorageError → std::error_code
std::error_code make_error_code(StorageError e);

class StorageErrorCategory : public std::error_category {
public:
    const char* name() const noexcept override { return "storage-engine"; }
    std::string message(int code) const override {
        auto e = static_cast<StorageError>(code);
        return module_name(e);
    }
};

}  // namespace storage

// 使 StorageError 可用于 std::error_code
namespace std {
template<> struct is_error_code_enum<storage::StorageError> : true_type {};
}
