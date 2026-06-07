#include "runtime.h"

namespace storage::runtime {

Runtime::Runtime(const RuntimeConfig& cfg)
    : online_group_(cfg.online_cfg),
      offline_group_(cfg.offline_cfg) {}

void Runtime::start() {
    online_group_.start();
    offline_group_.start();
}

void Runtime::stop() {
    offline_group_.stop();
    online_group_.stop();
}

}  // namespace storage::runtime
