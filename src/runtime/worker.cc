#include "worker.h"
#include "policy_factory.h"
#include <hwloc.h>

namespace storage::runtime {

std::atomic<size_t> Worker::next_id_{0};

Worker::Worker(Config cfg)
    : id_(next_id_++),
      cfg_(std::move(cfg)),
      idle_(std::make_unique<AdaptiveIdle>(cfg_.idle_cfg)),
      policy_(make_policy(cfg_.policy_cfg)),
      scheduler_(policy_.get(), idle_.get()) {}

size_t Worker::add_queue(std::unique_ptr<WorkQueue> q) {
    return scheduler_.register_queue(std::move(q));
}

void Worker::set_policy(std::unique_ptr<SchedulingPolicy> p) {
    scheduler_.set_policy(p.get());
    policy_ = std::move(p);
}

void Worker::start() {
    scheduler_.reset_stop();
    thread_ = std::thread([this] {
        worker_loop();
    });
    if (cfg_.cpu_id > 0) {
        hwloc_topology_t topo;
        hwloc_topology_init(&topo);
        hwloc_topology_load(topo);
        hwloc_cpuset_t cpuset = hwloc_bitmap_alloc();
        hwloc_bitmap_set(cpuset, cfg_.cpu_id - 1);
        hwloc_set_cpubind(topo, cpuset, HWLOC_CPUBIND_THREAD);
        hwloc_bitmap_free(cpuset);
        hwloc_topology_destroy(topo);
    }
}

void Worker::stop() {
    scheduler_.request_stop();
}

void Worker::join() {
    if (thread_.joinable()) {
        thread_.join();
    }
}

void Worker::notify() {
    idle_->notify();
}

WorkerStats Worker::stats() const {
    WorkerStats s;
    const auto& sched_stats = scheduler_.stats();
    s.tasks_executed = sched_stats.total_tasks_executed;
    s.total_exec_ns = sched_stats.total_exec_ns;
    s.total_polls = sched_stats.total_polls;
    s.total_idles = sched_stats.total_idles;
    return s;
}

void Worker::worker_loop() {
    on_worker_start();
    scheduler_.run();
}

}  // namespace storage::runtime
