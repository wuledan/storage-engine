#pragma once
#include "online_group.h"
#include "offline_group.h"
#include <memory>

namespace storage::runtime {

struct RuntimeConfig {
    OnlineWorkerGroup::Config online_cfg{4, 0};
    OfflineWorkerGroup::Config offline_cfg{2};
};

class Runtime {
public:
    explicit Runtime(const RuntimeConfig& cfg = {});

    void start();
    void stop();

    OnlineWorkerGroup& online_group() { return online_group_; }
    OfflineWorkerGroup& offline_group() { return offline_group_; }

private:
    OnlineWorkerGroup online_group_;
    OfflineWorkerGroup offline_group_;
};

}  // namespace storage::runtime
