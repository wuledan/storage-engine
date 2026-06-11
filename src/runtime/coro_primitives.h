#pragma once
// Coroutine primitives for the storage-engine framework.
// Single include for all coroutine coordination types.

#include "yield_awaiter.h"       // co_await yield() / yield_to()
#include "affinity_baton.h"      // co_await baton
#include "affinity_mutex.h"      // co_await mutex.co_lock()
#include "affinity_semaphore.h"  // co_await sem.acquire()
#include "work_item.h"           // WorkItem, make_coro, make_func
#include "worker.h"              // current_worker(), current_online_worker()
