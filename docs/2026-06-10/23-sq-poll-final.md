# SQPOLL Final Architecture — Non-Blocking io_uring Submission

## Configuration
- io_uring: SQPOLL + SQ_AFF, sq_thread_idle=0, sq_thread_cpu=2 (NUMA0)
- Worker: CPU1
- Test disk: /mnt/nvme_test/

## Key Principle
Non-blocking io_uring model: kernel SQPOLL thread handles SQE submission.
Zero io_uring_enter calls in the Scheduler hot path.
CQE detection via peek_cqe (userspace polling).

## Results at QD=1

| Backend    | flush  | co_await | producer | P50    | RIOP  |
|------------|--------|----------|----------|--------|-------|
| io_uring   | ~70ns  | ~32μs    | ~33μs    | ~30μs  | ~20K  |
| libaio     | ~30ns  | ~17μs    | ~26μs    | ~28μs  | ~26K  |

- flush: io_uring SQPOLL achieves zero-syscall submission (70ns vs libaio's io_submit at 8μs)
- co_await: includes IO latency + kernel thread scheduling + CQE detection + callback chain
- P50: matches fio baseline within expected framework + SQPOLL overhead
- Framework overhead confirmed near-zero: baton 47ns, Scheduler <1μs/iter

## tmpfs Validation (/dev/shm)
Memory-only test eliminates disk latency:

| Backend    | flush  | co_await | producer | P50    | RIOP    |
|------------|--------|----------|----------|--------|---------|
| io_uring   | 69ns   | 6.6μs   | 6.9μs   | 5.7μs  | 96.5K  |
| libaio     | 21ns   | 2.3μs   | 4.8μs   | 3.4μs  | 136.7K |

Disk latency: io_uring +29.9-5.7 ≈ 24μs, libaio +28.1-3.4 ≈ 24μs → consistent.

SQPOLL vs libaio gap: ~2μs kernel thread scheduling overhead on tmpfs.
This is the inherent cost of a separate kernel submission thread vs inline io_submit.

## Architecture Decision
- Non-blocking path: SQPOLL mandatory (no io_uring_enter in Scheduler)
- SINGLE_ISSUER: incompatible with our thread model (ring init on test thread, submit on Scheduler thread)
- Blocking approaches (submit_and_wait, GETEVENTS): rejected — block Scheduler thread
- One SQPOLL instance sufficient for single-disk bandwidth saturation

## Next Steps
- Multi-QD matrix (1, 4, 8, 16, 32, 64, 128, 256)
- Enterprise NVMe comparison
- IORING_SETUP_IOPOLL for latency-critical workloads
