#pragma once
#include "io_backend.h"
#include "io_engine.h"
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
#include <array>

// Forward-declare SPDK types — avoids dragging SPDK headers into consumers.
struct spdk_nvme_ctrlr;
struct spdk_nvme_ns;
struct spdk_nvme_qpair;
struct spdk_nvme_cpl;

namespace storage::io {

// ──────────────────────────────────────────────────────────────────────────────
// SPDKBackend — Userspace NVMe driver via SPDK.
//
// Architecture:
//   init:   spdk_env_init → spdk_nvme_probe → alloc qpair per worker
//   submit: spdk_nvme_ns_cmd_write/read (direct userspace, no syscall)
//   poll:   spdk_nvme_qpair_process_completions (userspace CQ read)
//
//   → zero kernel involvement, zero interrupts
//   → pure polling — ideal for coroutine model
//
// DMA buffers: all IO buffers must be allocated via alloc_dma_buffer()
// (hugepage-backed, physically contiguous for NVMe PRP lists).
// ──────────────────────────────────────────────────────────────────────────────
class SPDKBackend : public IIOBackend {
public:
    struct Config {
        std::string traddr;           // PCI address, e.g. "0000:5f:00.0"
        uint32_t nsid{1};            // namespace ID (default 1)
        size_t queue_depth{256};     // qpair size (SQ depth)
        size_t io_unit_size{0};      // LBA size in bytes (0 = auto-detect)

        // DMA buffer pool dimensions
        size_t buf_count{1024};
        size_t buf_size{4096};
    };

    // ── Construction ──
    //
    // Construct from generic IOBackendConfig.
    //   cfg.bdev_name   → PCI address (e.g. "0000:5f:00.0")
    //   cfg.queue_depth → NVMe queue depth
    //   cfg.max_file_size → namespace ID (if > 0 and ≤ UINT32_MAX)
    explicit SPDKBackend(const IOBackendConfig& cfg);
    ~SPDKBackend() override;

    std::string_view name() const noexcept override { return "spdk"; }

    void submit(IORequest req) override;
    size_t poll(IOCompletion* out, size_t max) override;

    // ── SPDK per-process initialization ──
    //
    // Must be called once before any SPDKBackend is constructed.
    // Safe to call multiple times (idempotent).
    // Returns 0 on success, -errno on failure.
    static int init_env();

    // ── DMA buffer helpers ──
    //
    // SPDK requires DMA-able memory backed by hugepages.  Use these to
    // allocate buffers that can be passed to submit().
    static void* alloc_dma_buffer(size_t size, size_t align = 4096);
    static void free_dma_buffer(void* buf, size_t size = 0);

private:
    Config cfg_;

    // ── SPDK handles ──
    spdk_nvme_ctrlr* ctrlr_{nullptr};
    spdk_nvme_ns* ns_{nullptr};
    spdk_nvme_qpair* qpair_{nullptr};

    // ── In-flight IO tracking ──
    //
    // pending_[idx] stores the IORequest submitted with sequence number idx.
    // The idx is passed as cb_arg to the SPDK NVMe command.
    std::vector<IORequest> pending_;
    size_t submit_count_{0};

    // ── Completion staging ──
    //
    // The SPDK completion callback (on_io_complete) fills this array.
    // poll() drains it into the caller-supplied out[] after calling
    // spdk_nvme_qpair_process_completions().
    //
    // This is an SPSC buffer with single-thread access: both the callback
    // and poll() execute on the same scheduler thread (the callback fires
    // synchronously inside process_completions).
    static constexpr size_t kCompletionBatch = 1024;
    std::array<IOCompletion, kCompletionBatch> completed_;
    size_t completed_count_{0};

    // ── DMA buffer pool ──
    std::vector<void*> dma_bufs_;

    // ── IO completion callback for SPDK ──
    //
    // SPDK calls this inside spdk_nvme_qpair_process_completions().
    static void on_io_complete(void* arg, const spdk_nvme_cpl* cpl);

    // ── Backend instance pointer ──
    //
    // SPDK's callback model passes a void* cb_arg per IO.  We use it to
    // carry the pending_ index.  The backend pointer is stored globally
    // since there is at most one SPDKBackend per process.
    static SPDKBackend* instance_;
};

}  // namespace storage::io
