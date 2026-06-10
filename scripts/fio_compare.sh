#!/bin/bash
# Compare our framework with fio at various QDs
set -euo pipefail
FILE=${1:-/mnt/nvme_test/fio_compare}
SIZE=${2:-1G}
ENGINE=${3:-io_uring}

echo "=== fio comparison ($ENGINE) ==="
for qd in 1 4 8 16 32 64 128 256; do
    echo "QD=$qd:"
    fio --name="qd$qd" --filename="$FILE" --direct=1 --rw=write --bs=4k \
        --ioengine="$ENGINE" --iodepth=$qd --size="$SIZE" --numjobs=1 \
        --group_reporting --minimal 2>/dev/null | \
        awk -F';' '{printf "  IOPS=%s  P50=%sus  P99=%sus  BW=%sKiB/s\n", $8, $18, $20, $7}'
    rm -f "$FILE"
    # Pre-create file for next QD
    dd if=/dev/zero of="$FILE" bs=4k count=1 oflag=direct 2>/dev/null
    dd if=/dev/zero of="$FILE" bs=4k count=1 seek=$((1024*1024/4 - 1)) oflag=direct 2>/dev/null
done
rm -f "$FILE"
