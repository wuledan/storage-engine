#include "spdk_backend.h"
#include "runtime/storage_error.h"
#include <cerrno>
#include <cstring>
#include <cstdio>
#include <stdexcept>
#include <algorithm>

// =============================================================================
//  Full SPDK implementation
//  Compiled only when HAS_SPDK is defined (SPDK detected by CMake).
//  Otherwise, stub implementations return -ENOSYS.
// =============================================================================

#ifdef HAS_SPDK

#include <atomic>

extern "C" {
#include <spdk/env.h>
#include <spdk/nvme.h>
#include <spdk/stdinc.h>
}

namespace storage::io {
namespace {

// ── Global SPDK init guard ──
std::atomic<bool> g_spdk_initialized{false};

// ── Probe context passed to probe_cb / attach_cb ──
struct ProbeCtx {
    SPDKBackend::Config cfg;
    spdk_nvme_ctrlr* ctrlr{nullptr};
    spdk_nvme_ns* ns{nullptr};
};

// ── NVMe probe callback (filter) ──
//
// Called by spdk_nvme_probe() for each discovered NVMe device.
// Returns true (= attach) only when the PCI address matches.
bool probe_cb(void* cb_ctx, const struct spdk_nvme_transport_id* trid,
              struct spdk_nvme_ctrlr_opts* opts) {
    auto* ctx = static_cast<ProbeCtx*>(cb_ctx);

    // Build PCI address string from the transport ID
    char addr[64];
    int n = snprintf(addr, sizeof(addr), "%04x:%02x:%02x.%x",
                     trid->traddr.pci.domain,
                     trid->traddr.pci.bus,
                     trid->traddr.pci.device,
                     trid->traddr.pci.function);
    if (n < 0) return false;

    bool match = (ctx->cfg.traddr == addr);
    if (match) {
        fprintf(stderr, "[SPDK] probe_cb: matched device at %s\n", addr);
    }
    return match;
}

// ── NVMe attach callback ──
//
// Called by spdk_nvme_probe() after a matching device has been attached.
void attach_cb(void* cb_ctx, const struct spdk_nvme_transport_id* trid,
               struct spdk_nvme_ctrlr* ctrlr,
               const struct spdk_nvme_ctrlr_opts* opts) {
    auto* ctx = static_cast<ProbeCtx*>(cb_ctx);

    // Take the first (and only) matching controller.
    if (!ctx->ctrlr) {
        ctx->ctrlr = ctrlr;

        // Validate the namespace
        ctx->ns = spdk_nvme_ctrlr_get_ns(ctrlr, ctx->cfg.nsid);
        if (!ctx->ns || !spdk_nvme_ns_is_active(ctx->ns)) {
            fprintf(stderr, "[SPDK] attach_cb: namespace %u not found/active\n",
                    ctx->cfg.nsid);
            ctx->ctrlr = nullptr;
            ctx->ns = nullptr;
        }
    }
}

}  // anonymous namespace

// ── Static member definitions ──
SPDKBackend* SPDKBackend::instance_{nullptr};

// ── on_io_complete ──
//
// Called by SPDK inside spdk_nvme_qpair_process_completions() for each
// completed I/O.  Fills the completion staging array for batch harvesting
// by poll().  Both submit() and poll() run on the same scheduler thread;
// the callback fires synchronously inside poll()'s call to
// process_completions().  No locking is needed.
void SPDKBackend::on_io_complete(void* arg, const spdk_nvme_cpl* cpl) {
    uint64_t idx = reinterpret_cast<uint64_t>(arg);
    SPDKBackend* self = instance_;
    if (!self || idx >= self->submit_count_) {
        return;  // stale completion after teardown
    }

    IOCompletion comp;
    comp.user_data = idx;

    if (spdk_nvme_cpl_is_error(cpl)) {
        comp.result = -EIO;
    } else {
        comp.result = static_cast<int64_t>(self->pending_[idx].len);
    }

    // Invoke callback inline (zero allocation on coroutine frame).
    // The callback fires synchronously inside poll()'s call to
    // process_completions() — same thread, no reentrancy issue.
    if (idx < self->pending_.size() && self->pending_[idx].callback_fn) {
        self->pending_[idx].callback_fn(self->pending_[idx].callback_ctx, comp);
    }

    if (self->completed_count_ < self->kCompletionBatch) {
        self->completed_[self->completed_count_++] = comp;
    } else {
        fprintf(stderr, "[SPDK] completion overflow! Dropping result.\n");
    }
}

// ── init_env ──
//
// One-time SPDK environment bootstrap.  Idempotent.
int SPDKBackend::init_env() {
    if (g_spdk_initialized.load(std::memory_order_acquire)) {
        return 0;
    }

    struct spdk_env_opts opts;
    spdk_env_opts_init(&opts);
    opts.name = "storage_engine";
    opts.core_mask = "0x1";
    opts.mem_size = 512;
    opts.shm_id = -1;
    opts.hugepage_single_segments = true;

    int rc = spdk_env_init(&opts);
    if (rc < 0) {
        fprintf(stderr, "[SPDK] env_init failed: %s\n", strerror(-rc));
        return -EIO;
    }

    g_spdk_initialized.store(true, std::memory_order_release);
    fprintf(stderr, "[SPDK] env initialized (mem=%d MB, core=%s)\n",
            opts.mem_size, opts.core_mask);
    return 0;
}

// ── DMA buffer helpers ──
void* SPDKBackend::alloc_dma_buffer(size_t size, size_t align) {
    return spdk_dma_malloc(size, align, nullptr);
}

void SPDKBackend::free_dma_buffer(void* buf, size_t /*size*/) {
    spdk_dma_free(buf);
}

}  // namespace storage::io

// =============================================================================
//  Constructor / destructor / submit / poll — separate section because they
//  need the anonymous-namespace helpers defined above.
// =============================================================================

namespace storage::io {

SPDKBackend::SPDKBackend(const IOBackendConfig& cfg)
    : cfg_() {

    // ── Convert IOBackendConfig → backend Config ──
    cfg_.traddr = cfg.bdev_name;
    cfg_.nsid = 1;
    if (cfg.max_file_size > 0 && cfg.max_file_size <= UINT32_MAX) {
        cfg_.nsid = static_cast<uint32_t>(cfg.max_file_size);
    }
    cfg_.queue_depth = cfg.queue_depth > 0 ? cfg.queue_depth : 256;
    cfg_.io_unit_size = 0;  // auto-detect

    if (cfg_.traddr.empty()) {
        throw std::runtime_error(
            "[SPDK] No PCI address provided. "
            "Set IOBackendConfig::bdev_name to the NVMe PCI address "
            "(e.g. \"0000:5f:00.0\").");
    }

    // ── Ensure SPDK environment is initialized ──
    int rc = init_env();
    if (rc != 0) {
        throw std::runtime_error(
            std::string("[SPDK] env_init failed: ") + strerror(-rc));
    }

    // ── Register this instance ──
    if (instance_) {
        throw std::runtime_error(
            "[SPDK] Only one SPDKBackend instance is allowed per process.");
    }
    instance_ = this;

    // ── Probe and attach NVMe device ──
    ProbeCtx probe_ctx{cfg_, nullptr, nullptr};
    rc = spdk_nvme_probe(
        nullptr,           // trid filter: NULL = all PCIe devices
        &probe_ctx,
        probe_cb,          // filter: match by PCI address
        attach_cb,         // store controller + namespace
        nullptr);          // remove_cb: unused
    if (rc != 0) {
        instance_ = nullptr;
        throw std::runtime_error(
            std::string("[SPDK] spdk_nvme_probe failed: ") + strerror(-rc));
    }
    if (!probe_ctx.ctrlr) {
        instance_ = nullptr;
        throw std::runtime_error(
            "[SPDK] No matching NVMe device found at " + cfg_.traddr);
    }
    if (!probe_ctx.ns) {
        instance_ = nullptr;
        spdk_nvme_detach(probe_ctx.ctrlr);
        throw std::runtime_error(
            "[SPDK] Namespace " + std::to_string(cfg_.nsid) +
            " not available on " + cfg_.traddr);
    }

    ctrlr_ = probe_ctx.ctrlr;
    ns_ = probe_ctx.ns;

    // ── Auto-detect IO unit size ──
    if (cfg_.io_unit_size == 0) {
        cfg_.io_unit_size = spdk_nvme_ns_get_sector_size(ns_);
        fprintf(stderr, "[SPDK] Auto-detected sector size: %zu bytes\n",
                cfg_.io_unit_size);
    }

    // ── Allocate I/O queue pair ──
    struct spdk_nvme_io_qpair_opts qp_opts;
    spdk_nvme_ctrlr_get_default_io_qpair_opts(ctrlr_, &qp_opts,
                                               sizeof(qp_opts));
    qp_opts.io_queue_size = static_cast<uint32_t>(cfg_.queue_depth);
    qp_opts.io_queue_requests = static_cast<uint32_t>(cfg_.queue_depth * 2);

    qpair_ = spdk_nvme_ctrlr_alloc_io_qpair(ctrlr_, &qp_opts, sizeof(qp_opts));
    if (!qpair_) {
        spdk_nvme_detach(ctrlr_);
        ctrlr_ = nullptr;
        instance_ = nullptr;
        throw std::runtime_error("[SPDK] qpair allocation failed");
    }

    // ── Initialize tracking structures ──
    pending_.reserve(cfg_.queue_depth * 2);
    pending_.resize(cfg_.queue_depth * 2);
    completed_.fill(IOCompletion{});

    // ── Pre-allocate DMA buffer pool ──
    dma_bufs_.reserve(cfg_.buf_count);
    for (size_t i = 0; i < cfg_.buf_count; ++i) {
        void* buf = spdk_dma_malloc(cfg_.buf_size, cfg_.io_unit_size, nullptr);
        if (!buf) {
            fprintf(stderr,
                    "[SPDK] DMA pool: only allocated %zu / %zu buffers\n",
                    i, cfg_.buf_count);
            break;
        }
        dma_bufs_.push_back(buf);
    }

    // ── Log namespace info ──
    uint64_t ns_sectors = spdk_nvme_ns_get_num_sectors(ns_);
    uint64_t ns_bytes = spdk_nvme_ns_get_size(ns_);

    fprintf(stderr,
            "[SPDK] Ready: %s | nsid=%u | qd=%zu | lba=%zu B | "
            "ns=%" PRIu64 " sectors (%" PRIu64 " MiB) | dma_bufs=%zu\n",
            cfg_.traddr.c_str(), cfg_.nsid,
            cfg_.queue_depth, cfg_.io_unit_size,
            ns_sectors, ns_bytes / (1024 * 1024),
            dma_bufs_.size());
}

SPDKBackend::~SPDKBackend() {
    if (qpair_) {
        spdk_nvme_qpair_process_completions(qpair_, 0);
        spdk_nvme_ctrlr_free_io_qpair(qpair_);
        qpair_ = nullptr;
    }
    if (ctrlr_) {
        spdk_nvme_detach(ctrlr_);
        ctrlr_ = nullptr;
    }
    for (auto* buf : dma_bufs_) {
        spdk_dma_free(buf);
    }
    dma_bufs_.clear();

    if (instance_ == this) {
        instance_ = nullptr;
    }
}

void SPDKBackend::submit(IORequest req) {
    uint64_t idx = submit_count_++;

    if (idx >= pending_.size()) {
        pending_.resize(std::max(pending_.size() * 2, idx + 1));
    }
    pending_[idx] = std::move(req);

    uint64_t lba = pending_[idx].offset / cfg_.io_unit_size;
    uint32_t lba_count = static_cast<uint32_t>(
        (pending_[idx].len + cfg_.io_unit_size - 1) / cfg_.io_unit_size);
    if (lba_count == 0) lba_count = 1;

    void* buf = pending_[idx].buf;

    int rc = 0;
    switch (pending_[idx].op) {
    case IORequest::kRead:
        rc = spdk_nvme_ns_cmd_read(
            ns_, qpair_, buf, lba, lba_count,
            on_io_complete, reinterpret_cast<void*>(idx), 0);
        break;
    case IORequest::kWrite:
        rc = spdk_nvme_ns_cmd_write(
            ns_, qpair_, buf, lba, lba_count,
            on_io_complete, reinterpret_cast<void*>(idx), 0);
        break;
    default:
        if (pending_[idx].callback_fn) {
            IOCompletion comp;
            comp.result = -EINVAL;
            comp.user_data = idx;
            pending_[idx].callback_fn(pending_[idx].callback_ctx, comp);
        }
        return;
    }

    if (rc != 0) {
        fprintf(stderr, "[SPDK] NVMe cmd failed at submit (rc=%d)\n", rc);
        if (pending_[idx].callback_fn) {
            IOCompletion comp;
            comp.result = -rc;
            comp.user_data = idx;
            pending_[idx].callback_fn(pending_[idx].callback_ctx, comp);
        }
    }
}

size_t SPDKBackend::poll(IOCompletion* out, size_t max) {
    completed_count_ = 0;
    int32_t ret = spdk_nvme_qpair_process_completions(qpair_, 0);
    if (ret < 0) {
        fprintf(stderr, "[SPDK] qpair_process_completions error: %" PRId32 "\n",
                ret);
        out[0].result = ret;
        out[0].user_data = 0;
        return 1;
    }
    if (ret == 0) return 0;

    size_t count = std::min(completed_count_, max);
    for (size_t i = 0; i < count; ++i) {
        out[i] = std::move(completed_[i]);
    }
    return count;
}

}  // namespace storage::io

#else  /* !HAS_SPDK — stub implementation */

namespace storage::io {

SPDKBackend* SPDKBackend::instance_{nullptr};

SPDKBackend::SPDKBackend(const IOBackendConfig& cfg)
    : cfg_() {
    // SPDK not available at compile time — will return -ENOSYS on use.
}

SPDKBackend::~SPDKBackend() = default;

void SPDKBackend::submit(IORequest req) {
    if (req.callback_fn) {
        IOCompletion comp;
        comp.result = -ENOSYS;
        comp.user_data = 0;
        req.callback_fn(req.callback_ctx, comp);
    }
}

size_t SPDKBackend::poll(IOCompletion* out, size_t max) {
    return 0;  // no completions possible
}

int SPDKBackend::init_env() { return -ENOSYS; }

void* SPDKBackend::alloc_dma_buffer(size_t /*size*/, size_t /*align*/) {
    return nullptr;
}

void SPDKBackend::free_dma_buffer(void* /*buf*/, size_t /*size*/) {}

}  // namespace storage::io

#endif  /* HAS_SPDK */
