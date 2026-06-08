#include "storage_error.h"

namespace storage {

const char* module_name(StorageError e) noexcept {
    uint32_t seg = (static_cast<uint32_t>(e) >> 16) & 0xFF;
    switch (seg) {
        case 0x01: return "generic";
        case 0x10: return "runtime";
        case 0x20: return "io";
        case 0x30: return "rpc";
        case 0x40: return "engine";
        default:   return "unknown";
    }
}

std::error_code make_error_code(StorageError e) {
    static struct : std::error_category {
        const char* name() const noexcept override { return "storage-engine"; }
        std::string message(int code) const override {
            return module_name(static_cast<StorageError>(code));
        }
    } cat;
    return {static_cast<int>(e), cat};
}

}  // namespace storage
