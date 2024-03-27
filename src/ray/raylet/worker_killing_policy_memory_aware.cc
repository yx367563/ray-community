// Copyright 2022 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/raylet/worker_killing_policy_memory_aware.h"

#include <gtest/gtest_prod.h>

#include <boost/container_hash/hash.hpp>
#include <unordered_map>

#include "absl/container/flat_hash_map.h"
#include "absl/time/time.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/raylet/worker.h"
#include "ray/raylet/worker_killing_policy.h"
#include "ray/raylet/worker_pool.h"

namespace ray {

namespace raylet {

int64_t getWorkerUsedMemory(const std::shared_ptr<WorkerInterface> worker,
                            const MemorySnapshot &system_memory) {
  auto pid = worker->GetProcess().GetId();
  int64_t used_memory = 0;
  const auto pid_entry = system_memory.process_used_bytes.find(pid);
  if (pid_entry != system_memory.process_used_bytes.end()) {
    used_memory = pid_entry->second;
  } else {
    RAY_LOG_EVERY_MS(INFO, 60000)
        << "Can't find memory usage for PID, reporting zero. PID: " << pid;
  }
  return used_memory;
}

MemoryAwareWorkerKillingPolicy::MemoryAwareWorkerKillingPolicy() {}

const std::pair<std::shared_ptr<WorkerInterface>, bool>
MemoryAwareWorkerKillingPolicy::SelectWorkerToKill(
    const std::vector<std::shared_ptr<WorkerInterface>> &workers,
    const MemorySnapshot &system_memory) const {
  if (workers.empty()) {
    RAY_LOG_EVERY_MS(INFO, 5000) << "Worker list is empty. Nothing can be killed";
    return std::make_pair(nullptr, /*should retry*/ false);
  }

  std::vector<std::shared_ptr<WorkerInterface>> sortedWorkers = workers;
  std::sort(sortedWorkers.begin(),
            sortedWorkers.end(),
            [&system_memory](const std::shared_ptr<WorkerInterface> &left,
                             const std::shared_ptr<WorkerInterface> &right) -> bool {
              int left_retriable =
                  left->GetAssignedTask().GetTaskSpecification().IsRetriable() ? 0 : 1;
              int right_retriable =
                  right->GetAssignedTask().GetTaskSpecification().IsRetriable() ? 0 : 1;

              if (left_retriable == right_retriable) {
                auto left_used_memory = getWorkerUsedMemory(left, system_memory);
                auto right_used_memory = getWorkerUsedMemory(right, system_memory);
                if (left_used_memory == right_used_memory) {
                  return left->GetAssignedTaskTime() > right->GetAssignedTaskTime();
                }
                return left_used_memory >
                       right_used_memory;  // prioritize tasks that actually occupy the
                                           // most memory
              }

              return left_retriable < right_retriable;  // prioritize retryable tasks
            });

  auto worker_to_kill = sortedWorkers.front();
  bool should_retry =
      worker_to_kill->GetAssignedTask().GetTaskSpecification().IsRetriable();
  return std::make_pair(worker_to_kill, should_retry);
}

}  // namespace raylet

}  // namespace ray
