# 内存池设计 — MemoryPool

> 面向需要修改或优化内存分配路径的后端开发者。
> 源码位置：`src/runtime/memory_pool.h`、`src/runtime/memory_pool.cc`
> 基准测试：`tests/stress/test_pool_bench.cpp`

---

## 目录

1. [概述](#1-概述)
2. [三级分层架构](#2-三级分层架构)
3. [L1: ThreadLocalCache](#3-l1-threadlocalcache)
4. [L2: SizeClassFreeList + ReturnRing](#4-l2-sizeclassfreelist--returnring)
5. [L3: Bump Allocator](#5-l3-bump-allocator)
6. [CentralFreeList](#6-centralfreelist)
7. [⚠️ TLS Cache 原子开销分析](#7-tls-cache-原子开销分析)
8. [MPMCRing](#8-mpmcring)
9. [co_allocate / co_deallocate 接口预留](#9-co_allocate--co_deallocate-接口预留)

---

## 1. 概述

`MemoryPool` 是 per-Worker 实例的内存池，为协程和工作线程提供低延迟、无锁的小对象分配。

```
MemoryPool (src/runtime/memory_pool.h:39-61)
  └── Impl  (src/runtime/memory_pool.cc:275-488)
        ├── L1: ThreadLocalCache           (TLS, 无原子, 单线程)
        ├── L2: SizeClassFreeList[6]       (无锁 CAS, 共享)
        │     └── ReturnRing[6]            (MPMC, TLS→central 批量回传)
        ├── L3: Bump Allocator             (预分配 1MB blocks)
        └── CentralFreeList                (共享, medium objects 4KB)
```

| 层级 | 作用域 | 同步 | 延迟 | 容量 |
|------|--------|------|------|------|
| L1: ThreadLocalCache | 线程本地 | 无（单线程） | ~5ns | 每 size class 上限 64 |
| L2: SizeClassFreeList | Worker 共享 | CAS（无锁） | ~20ns | 无上限 |
| L3: Bump Allocator | Worker 共享 | CAS（无锁，预分配） | ~10ns | 1MB block 递增 |
| CentralFreeList | Worker 共享 | CAS（无锁） | ~30ns | medium objects (256B–4KB) |

### Size Classes

```cpp
// memory_pool.cc:16
static constexpr size_t kSizeClasses[] = {8, 16, 32, 64, 128, 256};
static constexpr size_t kNumSizeClasses = 6;
static constexpr size_t kSmallObjectMax = 256;
static constexpr size_t kMediumObjectMax = 4096;
```

映射方式（`memory_pool.cc:33-48`）：

```cpp
static size_t size_class_index(size_t size) {
    if (size <= 8) return 0;
    size_t s = size - 1;
    // 向上取整到下一个 2 的幂
    s |= s >> 1;  s |= s >> 2;  s |= s >> 4;
    s |= s >> 8;  s |= s >> 16; s |= s >> 32;
    s += 1;
    // ctz: 8=2^3→idx 0, 16=2^4→idx 1, ..., 256=2^8→idx 5
    const size_t idx = __builtin_ctzll(s) - 3;
    return idx < kNumSizeClasses ? idx : kNumSizeClasses;
}
```

实际对象大小 = `kSizeClasses[size_class_index(alloc_size)]`（大于 256B 的对象不经过 size class 路径）。

### 分配路径总览

```
allocate(32B)
  │
  ├─ L1: ThreadLocalCache.free_lists_[2 → 32B].head ?
  │    有 → pop head, count--, return
  │    无 → 进入 L2
  │
  ├─ L2: 先从 ReturnRing[2] drain 到 SizeClassFreeList[2]
  │      再从 SizeClassFreeList[2]->pop()
  │    有 → return
  │    无 → 进入 L3
  │
  └─ L3: bump 指针分配 (当前 block)
       块够 → bump_offset += size, return
       不够 → 切到下一个 pre-allocated block / alloc_block()

deallocate(32B ptr)
  │
  ├─ 32B ≤ kSmallObjectMax ?
  │    是 → L1: tls_cache.deallocate(ptr, class_idx)
  │          tls_cache.list_count(class_idx) > 64 ?
  │            是 → spill 到 ReturnRing
  │            否 → 留在 TLS cache
  │    否 → 进入 medium / large 路径
```

---

## 2. 三级分层架构

### 为什么需要三级？

| 需求 | 单级方案的问题 | 三级方案 |
|------|---------------|---------|
| 极低延迟分配 | 所有线程竞争同一 free list → CAS 抖动 | L1 无锁无竞争 |
| 避免内存饥饿 | 纯 TLS cache → 某个线程 hoard 大量内存 | L2 共享，TLS 溢出阈值 64 |
| 大块分配效率 | 每次 `::operator new` → syscall | L3 bump 指针 ~5ns |
| 跨线程归还 | TLS 无法直接归还到其他线程的 L1 | ReturnRing (MPMC) 批量回传 |

### 数据流

```
分配:
  Thread A (worker)          Thread B (worker)
       │                          │
       │ allocate(32)             │ allocate(32)
       ▼                          ▼
   L1: hit? ──yes──> return   L1: hit? ──yes──> return
       │                          │
       no                         no
       │                          │
       ▼                          ▼
   L2: pop from                   L2: pop from
   SizeClassFreeList[2]           SizeClassFreeList[2]
       │                          │
       hit                   miss │
       │                          │
       ▼                          ▼
   return                     L3: bump_allocate
                                   │
                                   ▼
                               return

归还 (deallocate):
  Thread A                          Thread B
       │                              │
       ▼                              ▼
   L1: deallocate                  L1: deallocate
   count ≤ 64?                     count ≤ 64?
       │                              │
       yes                            no (溢出)
       │                              │
       ▼                              ▼
   head push (TLS)               ReturnRing[2].try_enqueue(ptr)
                                       │
                                  Thread A 下次 allocate 时 drain:
                                       │
                                       ▼
                                  ReturnRing[2].try_dequeue(drained)
                                  SizeClassFreeList[2].push(drained)
```

---

## 3. L1: ThreadLocalCache

```cpp
// memory_pool.cc:232-272
class ThreadLocalCache {
public:
    struct LocalFreeList {
        SizeClassFreeList::Node* head{nullptr};
        size_t count{0};    // ← 普通 size_t，非 atomic
    };

    void* allocate(size_t size_class_idx) {
        if (size_class_idx < kNumSizeClasses) {
            auto& list = free_lists_[size_class_idx];
            if (!list.head) return nullptr;
            auto* node = list.head;
            list.head = list.head->next;
            list.count--;
            return node;
        }
        return nullptr;
    }

    void deallocate(void* ptr, size_t size_class_idx) {
        if (size_class_idx < kNumSizeClasses) {
            auto* node = static_cast<SizeClassFreeList::Node*>(ptr);
            auto& list = free_lists_[size_class_idx];
            node->next = list.head;
            list.head = node;
            list.count++;
        }
    }

    size_t list_count(size_t class_idx) const { /* ... */ }

private:
    LocalFreeList free_lists_[kNumSizeClasses];  // 内嵌数组，无堆间接
};
```

### 设计要点

1. **`LocalFreeList free_lists_[6]` 内嵌数组**：固定 6 个 `LocalFreeList` 结构体，每个包含一个 `Node* head` 和 `size_t count`。数组直接嵌入 `ThreadLocalCache` 对象，无 `std::unique_ptr` / `std::vector` 间接——TLS 中所有数据是连续的。

2. **单线程安全**：`ThreadLocalCache` 只在当前 Worker 线程的 `thread_local` 变量中使用（`memory_pool.cc:327`）：
   ```cpp
   thread_local ThreadLocalCache tls_cache;
   ```
   所有 `head` 操作（`head = head->next`、`node->next = list.head; list.head = node`）是典型的多线程危险的单链表操作，但在 TLS 上下文中——只有一个读者和写者——完全安全。

3. **溢出到 MPMCRing**：当 `tls_cache.list_count(class_idx) > 64` 时（`memory_pool.cc:393`），deallocate 不再将对象放回 TLS，而是通过 `return_rings_[class_idx].try_enqueue(ptr)` 放回 MPMC ring 供其他线程或下次 L2 drain 取走。这防止了单线程 hoard 过多内存。

### 性能特征

- 分配：`head == nullptr` 检查（1 次 load）→ `head = head->next`（1 次 store）→ `count--`（1 次 sub）。**零原子指令。**
- 释放：`node->next = head; head = node`（2 次 store）→ `count++`（1 次 add）。**零原子指令。**
- TLS 命中时延迟约 3-5ns（寄存器或 L1 cache 中的值）。

---

## 4. L2: SizeClassFreeList + ReturnRing

### SizeClassFreeList

```cpp
// memory_pool.cc:127-164
class SizeClassFreeList {
public:
    struct Node {
        Node* next{nullptr};
    };

    void push(void* ptr) {
        auto* node = static_cast<Node*>(ptr);
        auto* old = head_.load(std::memory_order_acquire);
        do {
            node->next = old;
        } while (!head_.compare_exchange_weak(old, node,
                std::memory_order_release,
                std::memory_order_acquire));
        count_.fetch_add(1, std::memory_order_relaxed);
    }

    void* pop() {
        auto* old = head_.load(std::memory_order_acquire);
        while (old) {
            if (head_.compare_exchange_weak(old, old->next,
                    std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                count_.fetch_sub(1, std::memory_order_relaxed);
                return old;
            }
        }
        return nullptr;
    }

private:
    std::atomic<Node*> head_{nullptr};
    std::atomic<size_t> count_{0};
};
```

**CAS 无锁链表**：
- `push`：标准的 CAS 入栈（Treiber stack）。读 `head_`，写 `node->next = old`，CAS 写 `head_ = node`。
- `pop`：CAS 出栈。读 `head_`，CAS 将 `head_` 从 `old` 改为 `old->next`。
- `head_` 从裸指针改为 `std::atomic<Node*>`——这是跨 Worker 共享的必要同步。

### ReturnRing — TLS → Central 批量回传

```cpp
// memory_pool.cc:337-340
void* drained;
int drain_count = 0;
while (return_rings_[class_idx].try_dequeue(drained)) {
    small_free_lists_[class_idx]->push(drained);
    if (++drain_count >= 16) break;  // ← 每批上限 16，防饥饿
}
```

| 方向 | 路径 | 批量 |
|------|------|------|
| TLS → Central | `tls_cache.list_count > 64` → `return_rings_[class_idx].try_enqueue(ptr)` | 每次 1 个 |
| Central → TLS | `allocate()` 在 L1 miss 后 drain return_ring | 每批 ≤ 16 |

**防饥饿机制**：每次 `allocate()` 进入 L2 时，先 drain return_ring 但最多 16 个元素。这防止了：
1. 一个分配请求 drain 整个 ring（几十万个元素）
2. 导致其他 Worker 的 return 操作长时间等待
3. 本次 allocate 的延迟过高

---

## 5. L3: Bump Allocator

```cpp
// memory_pool.cc:433-466
void* allocate_from_block(size_t size, size_t alignment = 16) {
    size_t align = std::max(alignment, size_t{16});
    bump_offset_ = (bump_offset_ + align - 1) & ~(align - 1);
    size = (size + align - 1) & ~(align - 1);

    // Try current bump block
    if (bump_block_ptr_ && bump_offset_ + size <= kBlockSize) {
        void* ptr = bump_block_ptr_ + bump_offset_;
        bump_offset_ += size;
        stats_.total_allocated.fetch_add(size, std::memory_order_relaxed);
        return ptr;
    }

    // Check next pre-allocated block
    for (++bump_block_idx_; bump_block_idx_ < preallocated_blocks_.size(); ++bump_block_idx_) {
        // ...
    }

    // Allocate a new block (if all pre-allocated exhausted)
    char* new_block = alloc_block();
    preallocated_blocks_.push_back(new_block);
    bump_block_ptr_ = new_block;
    bump_offset_ = size;
    bump_block_idx_ = preallocated_blocks_.size() - 1;
    // ...
}
```

### 行为

| 条件 | 行为 |
|------|------|
| 当前 block 够用 | `bump_offset_ += size`，返回指针 |
| 当前 block 不够 | 切换到下一个 pre-allocated block |
| 所有 pre-allocated block 用完 | `alloc_block()` 分配新的 1MB block |

```cpp
// memory_pool.cc:23-25
static char* alloc_block() {
    return static_cast<char*>(::operator new(kBlockSize, std::align_val_t(kBlockAlignment)));
}
```

预分配大小：1MB，64 字节对齐（cache line 对齐）。

### 线程安全说明

当前 bump 指针操作（`bump_block_ptr_`、`bump_offset_`、`bump_block_idx_`）是 `Impl` 的普通成员变量（`memory_pool.cc:472-474`），**无原子保护**。但在当前架构中：
- `MemoryPool` 是 per-Worker 实例（`worker.h:108`）
- `allocate()` 入口在 `Impl::allocate()` 中调用 `allocate_from_block()`
- 单一 Worker 线程串行执行，不可能有并发

因此 L3 不需要同步。未来如果支持 NUMA 共享池（多个 Worker 共享一个 MemoryPool），则需要在 `co_allocate` 路径加锁。

### 碎片特性

Bump allocator 天生无碎片——所有对象在 block 中连续分配，deallocate 不归还给 bump（归还到 TLS/SizeClassFreeList），只有整个 block 在 `Impl` 析构时整体释放。这意味着：
- **分配延迟恒定**（没有空闲链表遍历）
- **空间局部性好**（同一协程连续分配的对象在内存上相邻）
- **长时间运行有浪费**（deallocate 的对象不回到 bump 路径，SizeClassFreeList 可能堆积大量空闲块）

---

## 6. CentralFreeList

```cpp
// memory_pool.cc:167-228
class CentralFreeList {
public:
    void push(void* ptr, size_t size) {
        auto* node = static_cast<FreeNode*>(ptr);
        node->size = size;
        auto* old = head_.load(std::memory_order_acquire);
        do {
            node->next = old;
        } while (!head_.compare_exchange_weak(old, node,
                std::memory_order_release,
                std::memory_order_acquire));
    }

    void* pop(size_t min_size) {
        // Drain entire chain via exchange(nullptr)
        auto* chain = head_.exchange(nullptr, std::memory_order_acq_rel);
        if (!chain) return nullptr;

        // Walk chain, unlink first node >= min_size
        FreeNode* found = nullptr;
        // ... traversal ...

        // Push remaining chain back
        if (chain) {
            FreeNode* tail = chain;
            while (tail->next) tail = tail->next;
            auto* old = head_.load(std::memory_order_acquire);
            do {
                tail->next = old;
            } while (!head_.compare_exchange_weak(old, chain,
                    std::memory_order_release,
                    std::memory_order_acquire));
        }
        return found;
    }

private:
    struct FreeNode {
        size_t size;
        FreeNode* next;
    };
    std::atomic<FreeNode*> head_{nullptr};
};
```

**设计要点**：

1. **`exchange(nullptr)` drain**：`pop()` 先摘除整个链表（`head_.exchange(nullptr, acq_rel)`），然后线性遍历查找合适的 `FreeNode`。这样避免了在遍历过程中持有锁或逐一 CAS 出队。

2. **CAS 推回**：遍历后剩余的节点链通过 CAS 一次性推回 `head_`。这里有一个细微的竞态：推回时其他线程可能已经 push 了新节点，因此需要 `tail->next = old` 再 CAS 写入 `head_ = chain`。

3. **只用于 medium objects**（`memory_pool.cc:359-368`）：
   ```cpp
   if (alloc_size <= kMediumObjectMax) {
       ptr = central_free_list_.pop(alloc_size);
       if (ptr) { /* cache hit */ return ptr; }
       // cache miss → ::operator new
       ptr = ::operator new(alloc_size);
   }
   ```
   256B–4KB 的对象走 `CentralFreeList`。大于 4KB 的对象直接 `::operator new` / `::operator delete`。

---

## 7. ⚠️ TLS Cache 原子开销分析

> **TL;DR**: 线程局部存储（TLS）上的原子操作是过度设计——它不必要地触发了 `lock xadd` 总线锁，而 glibc 的 tcache 使用普通整数操作。修复后性能反超 glibc。

### 7.1 发现问题

在早期的实现中，`LocalFreeList` 的 `count_` 被错误地声明为 `std::atomic<size_t>`：

```cpp
// 早期版本（已修复）:
struct LocalFreeList {
    SizeClassFreeList::Node* head{nullptr};
    std::atomic<size_t> count_{0};  // ← 问题：TLS 中的原子
};
```

基准测试 `test_pool_bench.cpp:70-97`（`SingleThreadRoundTrip`）的结果：

| 版本 | RoundTrip 32B | vs glibc |
|------|---------------|----------|
| 原子版（`std::atomic<size_t> count_`） | **80 ns** | glibc 72 ns → **慢 11%** |
| 非原子版（`size_t count_`） | **65 ns** | glibc 72 ns → **快 11%** |

注意：glibc 的 `::operator new` / `::operator delete` 本身使用 tcache（per-thread cache），在单线程场景下也几乎是无锁的。我们的内存池要证明自己的价值，必须在 TLS 路径上**超越 tcache**。

### 7.2 根因分析

`count_` 虽然位于 `thread_local` 存储中，但它的类型是 `std::atomic<size_t>`。编译器对 `count_.fetch_add(1, ...)` 生成：

```asm
lock xadd QWORD PTR [rdi+8], rax   ; 锁定内存总线
```

`lock xadd` 指令尽管目标地址在不同线程上是不同的 cache line，但 `lock` 前缀仍然：
1. **锁定内存总线**：~20 cycles 的额外开销
2. **阻止 CPU 重排序**：完全的内存屏障，远超 release 语义所需
3. **跨核心 cache 一致性流量**：即使 TLS 地址是线程私有的，`lock` 指令仍会触发缓存一致性协议的消息

而 `count++`（普通 `size_t`）生成：

```asm
add QWORD PTR [rdi+8], rax   ; 普通指令，~1 cycle
```

### 7.3 修复

```cpp
// 修复后 (memory_pool.cc:236):
struct LocalFreeList {
    SizeClassFreeList::Node* head{nullptr};
    size_t count{0};    // ← 普通 size_t，非 atomic
};
```

所有 `count_.fetch_add/sub` 改为 `count++/--`。

### 7.4 教训

**TLS 是单线程独占的**。任何 TLS 数据结构的原子操作都是过度设计。这包括：

| 场景 | 错误做法 | 正确做法 |
|------|---------|---------|
| TLS cache 计数 | `std::atomic<size_t>` | `size_t` |
| TLS queue head | `std::atomic<Node*>` | `Node*` |
| TLS 状态标志 | `std::atomic<bool>` | `bool` |
| 任何 thread_local 变量 | 任何 std::atomic | 普通类型 |

**例外**：TLS 变量如果可能被信号处理函数或 `atexit` 回调访问（需要 `memory_order_relaxed` 的原子读），但本项目不使用信号。

### 7.5 性能对比数据

`test_pool_bench.cpp` 中的完整对比（测试环境：Intel Xeon Gold 6248, gcc 12.2）：

| Benchmark | MemoryPool | glibc | 加速比 |
|-----------|-----------|-------|--------|
| AllocOnly 64B | 7.2 ns/op | 14.8 ns/op | **2.1x** |
| RoundTrip 32B | 65 ns | 72 ns | **1.1x** |
| RandomSize Alloc | 9.1 ns/op | — | — |
| SteadyState 64B | 8.3 ns/op | — | — |
| MultiThread (4T) | 95 ns/roundtrip | 340 ns | **3.6x** |

多线程场景的加速比更大，因为 `MemoryPool` 的 L2/L3 使用无锁算法，而 glibc 的 `malloc` 在高度竞争下退化为 `futex` 系统调用。

### 7.6 与 glibc tcache 的设计差异

| 维度 | glibc tcache | 本项目 MemoryPool |
|------|-------------|-------------------|
| 每 size class cache 上限 | 7（tcache_count） | 64（溢出到 return ring） |
| 跨线程归还 | tcache 不直接支持 | ReturnRing (MPMC) |
| cache miss 后路径 | bins → top chunk | SizeClassFreeList → Bump Allocator |
| 线程模型 | 通用 | 专属 Worker 线程（可定制） |
| 可观测性 | 无 | MemoryPoolStats（命中率、延迟分布） |

---

## 8. MPMCRing

### 设计：Vyukov 风格有界 MPMC 队列

```cpp
// memory_pool.cc:50-124
template<size_t Capacity = 256>
class MPMCRing {
    struct Cell {
        std::atomic<size_t> sequence{0};
        std::atomic<void*> data{nullptr};
    };

    static constexpr size_t kMask = Capacity - 1;
    std::array<Cell, Capacity> buffer_;
    alignas(64) std::atomic<size_t> head_{0};
    alignas(64) std::atomic<size_t> tail_{0};
    // ...
};
```

基于 Dmitry Vyukov 的有界 MPMC 队列设计（[原始论文](https://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue)）。

### 序列号防 ABA

每个 cell 包含一个 `sequence` 计数器：

```
初始状态：sequence[0] = 0, sequence[1] = 1, ..., sequence[N-1] = N-1
                    head_ = 0, tail_ = 0

enqueue: 取 tail_，cell.sequence == tail 才可写入
         写入 data，设置 sequence = tail + 1

dequeue: 取 head_，cell.sequence == head + 1 才可读取
         读取 data，设置 sequence = head + Capacity
```

`sequence` 字段替代了传统的 `std::atomic` 指针比较，**完全避免了 ABA 问题**——即使同一个内存地址被复用，它的 sequence 值也已经改变。

### `try_enqueue`

```cpp
// memory_pool.cc:76-98
bool try_enqueue(void* ptr) noexcept {
    size_t tail = tail_.load(std::memory_order_relaxed);
    for (;;) {
        Cell& cell = buffer_[tail & kMask];
        size_t seq = cell.sequence.load(std::memory_order_acquire);
        intptr_t diff = static_cast<intptr_t>(seq) -
                        static_cast<intptr_t>(tail);
        if (diff < 0) return false;         // full
        if (diff != 0) {
            tail = tail_.load(std::memory_order_relaxed);  // 竞争失败，重试
            continue;
        }
        if (tail_.compare_exchange_weak(tail, tail + 1,
                std::memory_order_relaxed, std::memory_order_relaxed)) {
            cell.data.store(ptr, std::memory_order_release);
            cell.sequence.store(tail + 1, std::memory_order_release);
            return true;
        }
    }
}
```

### `try_dequeue`

```cpp
// memory_pool.cc:102-123
bool try_dequeue(void*& ptr) noexcept {
    size_t head = head_.load(std::memory_order_relaxed);
    for (;;) {
        Cell& cell = buffer_[head & kMask];
        size_t seq = cell.sequence.load(std::memory_order_acquire);
        intptr_t diff = static_cast<intptr_t>(seq) -
                        static_cast<intptr_t>(head + 1);
        if (diff < 0) return false;         // empty
        if (diff != 0) {
            head = head_.load(std::memory_order_relaxed);
            continue;
        }
        if (head_.compare_exchange_weak(head, head + 1,
                std::memory_order_relaxed, std::memory_order_relaxed)) {
            ptr = cell.data.load(std::memory_order_acquire);
            cell.sequence.store(head + Capacity, std::memory_order_release);
            return true;
        }
    }
}
```

### tail 等待循环（DPDK 原版机制）

`try_enqueue` 和 `try_dequeue` 都使用**等待循环**（不是 busy-wait）：

- `try_enqueue`：当 `diff != 0` 时（其他线程竞争领先），重新加载 `tail_` 重试。`diff < 0` 表示队列已满，返回 `false`。
- `try_dequeue`：类似，`diff < 0` 表示队列为空。

这与 DPDK 的 rte_ring 原版机制一致——**无锁无误导**（no false-sharing, no backoff）。

### 用途

`MemoryPool` 中 `return_rings_` 是 `MPMCRing<>` 类型的数组，每个 size class 一个：

```cpp
// memory_pool.cc:470
MPMCRing<> return_rings_[kNumSizeClasses];
```

容量默认 256（每个 size class），在 `try_enqueue` 返回 false 时说明 ring 已满——此时归还到 TLS 的对象会暂存在 L1 中（即溢出阈值 64 已到，但 return ring 也满了）。这在实际负载下极少发生，因为每次 allocate 会 drain ring。

---

## 9. co_allocate / co_deallocate 接口预留

### 声明

```cpp
// memory_pool.h:48-50
// ── Coroutine interface (per-NUMA shared, needs co_lock in future) ──
// TODO: co_allocate/co_deallocate with AffinityMutex co_lock for NUMA pools
// For now, these delegate to the sync path since per-Worker is single-threaded.
```

### 未来设计

当需要**跨 Worker 共享 NUMA 内存池**时（多个 Worker 线程共享一个 `MemoryPool` 实例），当前的 per-Worker 单线程路径不再安全。计划中的接口：

```cpp
// 预留接口（尚未实现）
class MemoryPool {
    // 协程感知分配 — 使用 AffinityMutex 保护共享状态
    std::coroutine_handle<> co_allocate(size_t bytes, size_t alignment,
                                         void*& out_ptr);
    std::coroutine_handle<> co_deallocate(void* ptr, size_t bytes,
                                           size_t alignment);

    // 使用方式：
    //   void* ptr;
    //   co_await pool.co_allocate(64, 16, ptr);
    //   // ... use ptr ...
    //   co_await pool.co_deallocate(ptr, 64, 16);
};
```

**设计思路**：

```cpp
class MemoryPool {
    AffinityMutex co_mutex_;  // 保护共享分配路径

    // allocate_from_block 和 central_free_list_ 需要互斥
};
```

`co_allocate` 的实现将使用 `co_await co_mutex_.co_scoped_lock()` 保护 bump 分配和 central free list 操作，确保在协程挂起时释放锁——这与 `AffinityMutex` 的设计一致。

### 当前回退

```cpp
// memory_pool.cc:503-508
void* MemoryPool::allocate(size_t bytes, size_t alignment) {
    return impl_->allocate(bytes, alignment);
}

void MemoryPool::deallocate(void* ptr, size_t bytes, size_t alignment) {
    impl_->deallocate(ptr, bytes, alignment);
}
```

当前路径是同步的、per-Worker 单线程安全的。未来启用 NUMA 共享时：
1. `co_allocate`/`co_deallocate` 实现加锁版本
2. 同步 `allocate`/`deallocate` 保留用于单 Worker 内部路径（如 Worker 启动时初始化）
3. 运行时根据配置选择路径
