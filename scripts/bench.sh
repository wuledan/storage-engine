#!/bin/bash
# IO Benchmark Runner — runs CoroutinePipeline and collects results
# Usage: ./scripts/bench.sh [--qd "1,4,8,16"] [--filter "CoroutinePipeline"]
set -euo pipefail

QDS="${QDS:-1,4,8,16,32,64,128,256}"
FILTER="${FILTER:-CoroutinePipeline}"
BUILD_DIR="${BUILD_DIR:-build}"

echo "=== Building ==="
cd "$(dirname "$0")/.."
cmake -B "$BUILD_DIR" -S . > /dev/null
cmake --build "$BUILD_DIR" --target test_benchmark -j$(nproc)

echo "=== Running benchmark ($FILTER, QDs=$QDS) ==="
echo "File: /mnt/nvme_test/bench_c_io_uring"
echo ""

# Modify QDs if needed
if [ "$QDS" != "1,4,8,16,32,64,128,256" ]; then
    echo "Note: To change QDs, edit tests/stress/test_benchmark.cpp"
    echo "  std::vector<int> qds = {$QDS};"
    echo ""
fi

"$BUILD_DIR/tests/stress/test_benchmark" --gtest_filter="*${FILTER}*" 2>&1 | tee /tmp/bench_result.txt

echo ""
echo "=== Summary ==="
grep -E '^  [0-9]|RIOP|P50|Probe' /tmp/bench_result.txt | head -50

echo ""
echo "=== fio comparison (optional) ==="
echo "fio --name=test --filename=/mnt/nvme_test/fio_test --direct=1 --rw=write --bs=4k --ioengine=io_uring --iodepth=\$QD --size=1G --numjobs=1 --group_reporting"
