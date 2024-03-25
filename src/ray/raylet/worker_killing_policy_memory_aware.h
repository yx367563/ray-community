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

#pragma once

#include <gtest/gtest_prod.h>

#include "absl/container/flat_hash_set.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "ray/common/memory_monitor.h"
#include "ray/raylet/worker.h"
#include "ray/raylet/worker_killing_policy.h"

namespace ray {

namespace raylet {

/// Prioritize retryable tasks, sort them according to the actual memory occupied by the
/// tasks, and kill the task that occupy the most memory first.
///
/// When selecting a worker / task to be killed, it will set the task to-be-killed to be
/// non-retriable if it is the last member of the group, and is retriable otherwise.
class MemoryAwareWorkerKillingPolicy : public WorkerKillingPolicy {
 public:
  MemoryAwareWorkerKillingPolicy();
  const std::pair<std::shared_ptr<WorkerInterface>, bool> SelectWorkerToKill(
      const std::vector<std::shared_ptr<WorkerInterface>> &workers,
      const MemorySnapshot &system_memory) const;
};

}  // namespace raylet

}  // namespace ray
