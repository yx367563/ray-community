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

#include "gtest/gtest.h"
#include "ray/common/task/task_spec.h"
#include "ray/raylet/test/util.h"
#include "ray/raylet/worker_killing_policy.h"
#include "ray/util/process.h"

namespace ray {

class WorkerKillingMemoryAwareTest : public ::testing::Test {
 protected:
  instrumented_io_context io_context_;
  int32_t port_ = 2389;
  JobID job_id_ = JobID::FromInt(75);
  bool should_retry_ = true;
  bool should_not_retry_ = false;
  int32_t no_retry_ = 0;
  int32_t has_retry_ = 1;
  raylet::MemoryAwareWorkerKillingPolicy worker_killing_policy_;

  std::shared_ptr<raylet::WorkerInterface> CreateActorCreationWorker(
      TaskID owner_id, int32_t max_restarts) {
    rpc::TaskSpec message;
    message.set_task_id(TaskID::FromRandom(job_id_).Binary());
    message.set_parent_task_id(owner_id.Binary());
    message.mutable_actor_creation_task_spec()->set_max_actor_restarts(max_restarts);
    message.set_type(ray::rpc::TaskType::ACTOR_CREATION_TASK);
    TaskSpecification task_spec(message);
    RayTask task(task_spec);
    auto worker =
        std::make_shared<raylet::MockWorker>(ray::WorkerID::FromRandom(), port_);
    worker->SetAssignedTask(task);
    worker->AssignTaskId(task.GetTaskSpecification().TaskId());
    return worker;
  }

  std::shared_ptr<raylet::WorkerInterface> CreateTaskWorker(TaskID owner_id,
                                                            int32_t max_retries) {
    rpc::TaskSpec message;
    message.set_task_id(TaskID::FromRandom(job_id_).Binary());
    message.set_parent_task_id(owner_id.Binary());
    message.set_max_retries(max_retries);
    message.set_type(ray::rpc::TaskType::NORMAL_TASK);
    TaskSpecification task_spec(message);
    RayTask task(task_spec);
    auto worker =
        std::make_shared<raylet::MockWorker>(ray::WorkerID::FromRandom(), port_);
    worker->SetAssignedTask(task);
    worker->AssignTaskId(task.GetTaskSpecification().TaskId());
    return worker;
  }
};

TEST_F(WorkerKillingMemoryAwareTest, TestEmptyWorkerPoolSelectsNullWorker) {
  std::vector<std::shared_ptr<raylet::WorkerInterface>> workers;
  auto worker_to_kill_and_should_retry_ =
      worker_killing_policy_.SelectWorkerToKill(workers, MemorySnapshot());
  auto worker_to_kill = worker_to_kill_and_should_retry_.first;
  ASSERT_TRUE(worker_to_kill == nullptr);
}

TEST_F(WorkerKillingMemoryAwareTest, TestSortedByRetriable) {
  std::vector<std::shared_ptr<raylet::WorkerInterface>> workers;
  auto first_submitted = WorkerKillingMemoryAwareTest::CreateActorCreationWorker(
      TaskID::FromRandom(job_id_), has_retry_);
  auto second_submitted = WorkerKillingMemoryAwareTest::CreateTaskWorker(
      TaskID::FromRandom(job_id_), no_retry_);
  auto third_submitted = WorkerKillingMemoryAwareTest::CreateTaskWorker(
      TaskID::FromRandom(job_id_), has_retry_);
  auto fourth_submitted = WorkerKillingMemoryAwareTest::CreateActorCreationWorker(
      TaskID::FromRandom(job_id_), no_retry_);
  workers.push_back(first_submitted);
  workers.push_back(second_submitted);
  workers.push_back(third_submitted);
  workers.push_back(fourth_submitted);

  std::vector<std::pair<std::shared_ptr<raylet::WorkerInterface>, bool>> expected;
  expected.push_back(std::make_pair(third_submitted, should_retry_));
  expected.push_back(std::make_pair(first_submitted, should_retry_));
  expected.push_back(std::make_pair(fourth_submitted, should_not_retry_));
  expected.push_back(std::make_pair(second_submitted, should_not_retry_));

  for (const auto &entry : expected) {
    auto worker_to_kill_and_should_retry_ =
        worker_killing_policy_.SelectWorkerToKill(workers, MemorySnapshot());
    auto worker_to_kill = worker_to_kill_and_should_retry_.first;
    bool retry = worker_to_kill_and_should_retry_.second;
    ASSERT_EQ(worker_to_kill->WorkerId(), entry.first->WorkerId());
    ASSERT_EQ(retry, entry.second);
    workers.erase(std::remove(workers.begin(), workers.end(), worker_to_kill),
                  workers.end());
  }
}

TEST_F(WorkerKillingMemoryAwareTest, TestSortedByMemory) {
  std::vector<std::shared_ptr<raylet::WorkerInterface>> workers;
  auto first_submitted = WorkerKillingMemoryAwareTest::CreateActorCreationWorker(
      TaskID::FromRandom(job_id_), has_retry_);
  first_submitted->SetProcess(Process::FromPid(PID_MAX_LIMIT + 1));
  auto second_submitted = WorkerKillingMemoryAwareTest::CreateTaskWorker(
      TaskID::FromRandom(job_id_), has_retry_);
  second_submitted->SetProcess(Process::FromPid(PID_MAX_LIMIT + 2));
  auto third_submitted = WorkerKillingMemoryAwareTest::CreateTaskWorker(
      TaskID::FromRandom(job_id_), has_retry_);
  third_submitted->SetProcess(Process::FromPid(PID_MAX_LIMIT + 3));
  auto fourth_submitted = WorkerKillingMemoryAwareTest::CreateActorCreationWorker(
      TaskID::FromRandom(job_id_), has_retry_);
  fourth_submitted->SetProcess(Process::FromPid(PID_MAX_LIMIT + 4));
  auto fifth_submitted = WorkerKillingMemoryAwareTest::CreateActorCreationWorker(
      TaskID::FromRandom(job_id_), has_retry_);
  fifth_submitted->SetProcess(Process::FromPid(PID_MAX_LIMIT + 5));
  auto sixth_submitted = WorkerKillingMemoryAwareTest::CreateTaskWorker(
      TaskID::FromRandom(job_id_), has_retry_);
  sixth_submitted->SetProcess(Process::FromPid(PID_MAX_LIMIT + 6));
  workers.push_back(first_submitted);
  workers.push_back(second_submitted);
  workers.push_back(third_submitted);
  workers.push_back(fourth_submitted);
  workers.push_back(fifth_submitted);
  workers.push_back(sixth_submitted);

  auto memory_snapshot = MemorySnapshot();
  memory_snapshot.process_used_bytes[first_submitted->GetProcess().GetId()] = 1024 * 10;
  memory_snapshot.process_used_bytes[second_submitted->GetProcess().GetId()] = 1024 * 1;
  memory_snapshot.process_used_bytes[third_submitted->GetProcess().GetId()] = 1024 * 5;
  memory_snapshot.process_used_bytes[fourth_submitted->GetProcess().GetId()] = 1024 * 5;
  memory_snapshot.process_used_bytes[fifth_submitted->GetProcess().GetId()] = 1024 * 50;
  memory_snapshot.process_used_bytes[sixth_submitted->GetProcess().GetId()] = 1024 * 25;

  std::vector<std::pair<std::shared_ptr<raylet::WorkerInterface>, bool>> expected;
  expected.push_back(std::make_pair(fifth_submitted, should_retry_));
  expected.push_back(std::make_pair(sixth_submitted, should_retry_));
  expected.push_back(std::make_pair(first_submitted, should_retry_));
  expected.push_back(std::make_pair(fourth_submitted, should_retry_));
  expected.push_back(std::make_pair(third_submitted, should_retry_));
  expected.push_back(std::make_pair(second_submitted, should_retry_));

  for (const auto &entry : expected) {
    auto worker_to_kill_and_should_retry_ =
        worker_killing_policy_.SelectWorkerToKill(workers, memory_snapshot);
    auto worker_to_kill = worker_to_kill_and_should_retry_.first;
    bool retry = worker_to_kill_and_should_retry_.second;
    ASSERT_EQ(worker_to_kill->WorkerId(), entry.first->WorkerId());
    ASSERT_EQ(retry, entry.second);
    workers.erase(std::remove(workers.begin(), workers.end(), worker_to_kill),
                  workers.end());
  }
}

TEST_F(WorkerKillingMemoryAwareTest, TestSortedByRetriableAndMemory) {
  std::vector<std::shared_ptr<raylet::WorkerInterface>> workers;
  auto first_submitted = WorkerKillingMemoryAwareTest::CreateActorCreationWorker(
      TaskID::FromRandom(job_id_), has_retry_);
  first_submitted->SetProcess(Process::FromPid(PID_MAX_LIMIT + 1));
  auto second_submitted = WorkerKillingMemoryAwareTest::CreateTaskWorker(
      TaskID::FromRandom(job_id_), no_retry_);
  second_submitted->SetProcess(Process::FromPid(PID_MAX_LIMIT + 2));
  auto third_submitted = WorkerKillingMemoryAwareTest::CreateTaskWorker(
      TaskID::FromRandom(job_id_), has_retry_);
  third_submitted->SetProcess(Process::FromPid(PID_MAX_LIMIT + 3));
  auto fourth_submitted = WorkerKillingMemoryAwareTest::CreateActorCreationWorker(
      TaskID::FromRandom(job_id_), has_retry_);
  fourth_submitted->SetProcess(Process::FromPid(PID_MAX_LIMIT + 4));
  auto fifth_submitted = WorkerKillingMemoryAwareTest::CreateActorCreationWorker(
      TaskID::FromRandom(job_id_), no_retry_);
  fifth_submitted->SetProcess(Process::FromPid(PID_MAX_LIMIT + 5));
  auto sixth_submitted = WorkerKillingMemoryAwareTest::CreateTaskWorker(
      TaskID::FromRandom(job_id_), has_retry_);
  sixth_submitted->SetProcess(Process::FromPid(PID_MAX_LIMIT + 6));
  workers.push_back(first_submitted);
  workers.push_back(second_submitted);
  workers.push_back(third_submitted);
  workers.push_back(fourth_submitted);
  workers.push_back(fifth_submitted);
  workers.push_back(sixth_submitted);

  auto memory_snapshot = MemorySnapshot();
  memory_snapshot.process_used_bytes[first_submitted->GetProcess().GetId()] = 1024 * 10;
  memory_snapshot.process_used_bytes[second_submitted->GetProcess().GetId()] = 1024 * 1;
  memory_snapshot.process_used_bytes[third_submitted->GetProcess().GetId()] = 1024 * 5;
  memory_snapshot.process_used_bytes[fourth_submitted->GetProcess().GetId()] = 1024 * 5;
  memory_snapshot.process_used_bytes[fifth_submitted->GetProcess().GetId()] = 1024 * 50;
  memory_snapshot.process_used_bytes[sixth_submitted->GetProcess().GetId()] = 1024 * 25;

  std::vector<std::pair<std::shared_ptr<raylet::WorkerInterface>, bool>> expected;
  expected.push_back(std::make_pair(sixth_submitted, should_retry_));
  expected.push_back(std::make_pair(first_submitted, should_retry_));
  expected.push_back(std::make_pair(fourth_submitted, should_retry_));
  expected.push_back(std::make_pair(third_submitted, should_retry_));
  expected.push_back(std::make_pair(fifth_submitted, should_not_retry_));
  expected.push_back(std::make_pair(second_submitted, should_not_retry_));

  for (const auto &entry : expected) {
    auto worker_to_kill_and_should_retry_ =
        worker_killing_policy_.SelectWorkerToKill(workers, memory_snapshot);
    auto worker_to_kill = worker_to_kill_and_should_retry_.first;
    bool retry = worker_to_kill_and_should_retry_.second;
    ASSERT_EQ(worker_to_kill->WorkerId(), entry.first->WorkerId());
    ASSERT_EQ(retry, entry.second);
    workers.erase(std::remove(workers.begin(), workers.end(), worker_to_kill),
                  workers.end());
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
