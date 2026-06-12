#include <gtest/gtest.h>
#include "runtime/metric_server.h"
#include "runtime/metric_counter.h"
#include <thread>
#include <chrono>

using namespace storage::runtime;
using namespace storage::runtime::metric;

TEST(MetricServer, StartStop) {
    MetricServer::Config cfg{10999};  // non-standard port
    MetricServer srv(cfg);
    srv.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_NO_THROW(srv.stop());
}

TEST(MetricServer, HealthEndpoint) {
    MetricServer::Config cfg{10998};
    MetricServer srv(cfg);
    srv.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Use curl or simple socket to check /health
    // For now, just verify server starts/stops cleanly
    EXPECT_NO_THROW(srv.stop());
}
