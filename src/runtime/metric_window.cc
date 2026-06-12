#include "metric_window.h"
#include <thread>

namespace storage::runtime::metric {

static std::thread g_exporter_thread;

void MetricExporter::start(std::chrono::milliseconds interval) {
    auto& inst = instance();
    inst.running_.store(true);
    g_exporter_thread = std::thread([&inst, interval] {
        while (inst.running_.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(interval);
            std::lock_guard<std::mutex> lk(inst.mutex_);
            for (auto& [name, rate] : inst.rates_) {
                // Rates are updated externally — exporter just sleeps
                // The actual rate.update() should be called by the metric producer
            }
        }
    });
}

void MetricExporter::stop() {
    instance().running_.store(false);
    if (g_exporter_thread.joinable()) g_exporter_thread.join();
}

void MetricExporter::register_rate(const std::string& name, MetricRate* rate) {
    std::lock_guard<std::mutex> lk(instance().mutex_);
    instance().rates_[name] = rate;
}

MetricExporter& MetricExporter::instance() {
    static MetricExporter inst;
    return inst;
}

std::string MetricExporter::to_json() {
    std::string out = "{";
    auto& inst = instance();
    std::lock_guard<std::mutex> lk(inst.mutex_);
    bool first = true;
    for (auto& [name, rate] : inst.rates_) {
        if (!first) out += ",";
        out += "\"" + name + "\":" + rate->to_json();
        first = false;
    }
    out += "}";
    return out;
}

std::string MetricExporter::to_prometheus() {
    std::string out;
    auto& inst = instance();
    std::lock_guard<std::mutex> lk(inst.mutex_);
    for (auto& [name, rate] : inst.rates_) {
        std::string n = name;
        for (auto& c : n) if (c == '/') c = '_';
        out += "# HELP " + n + " auto-generated\n";
        out += "# TYPE " + n + " gauge\n";
        out += n + " " + std::to_string(rate->value()) + "\n";
    }
    return out;
}

}  // namespace storage::runtime::metric
