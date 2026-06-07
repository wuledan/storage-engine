#include "worker.h"
#include "policy_factory.h"
#include <pthread.h>
#include <sched.h>

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
    policy_ = std::move(p);
}

void Worker::start() {
    thread_ = std::thread([this] {
        worker_loop();
    });
    if (cfg_.cpu_id > 0) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(cfg_.cpu_id - 1, &cpuset);
        pthread_setaffinity_np(thread_.native_handle(), sizeof(cpu_set_t), &cpuset);
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
