/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/table.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/avro/avro_register.h"
#include "iceberg/constants.h"
#include "iceberg/file_io.h"
#include "iceberg/inspect/metadata_scan_task_reader.h"
#include "iceberg/inspect/metadata_table.h"
#include "iceberg/inspect/parallel_metadata_poc_internal.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/arrow_test_util.h"
#include "iceberg/test/executor.h"
#include "iceberg/test/inspect/metadata_table_test_base.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_io.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg::internal {
namespace {

/// The first two manifest opens rendezvous, proving concurrent worker reads rather
/// than relying on elapsed-time assertions. A sequential implementation times out.
class ProbedFileIO : public FileIO {
 public:
  ProbedFileIO(std::shared_ptr<FileIO> delegate, bool require_overlap)
      : delegate_(std::move(delegate)), require_overlap_(require_overlap) {}

  Result<std::unique_ptr<InputFile>> NewInputFile(std::string path) override {
    ICEBERG_RETURN_UNEXPECTED(OnOpen());
    return delegate_->NewInputFile(std::move(path));
  }

  Result<std::unique_ptr<InputFile>> NewInputFile(std::string path,
                                                  size_t length) override {
    ICEBERG_RETURN_UNEXPECTED(OnOpen());
    return delegate_->NewInputFile(std::move(path), length);
  }

  int open_count() const { return opens_.load(); }

  int peak() const {
    std::lock_guard lock(mutex_);
    return peak_;
  }

 private:
  Status OnOpen() {
    opens_.fetch_add(1);
    if (!require_overlap_) {
      return {};
    }
    std::unique_lock lock(mutex_);
    ++active_;
    peak_ = std::max(peak_, active_);
    ready_.notify_all();
    const bool overlapped =
        ready_.wait_for(lock, std::chrono::seconds(5), [&] { return peak_ >= 2; });
    --active_;
    if (!overlapped) {
      return IOError("Manifest tasks did not execute concurrently");
    }
    return {};
  }

  const std::shared_ptr<FileIO> delegate_;
  const bool require_overlap_;
  std::atomic<int> opens_{0};
  mutable std::mutex mutex_;
  std::condition_variable ready_;
  int active_ = 0;
  int peak_ = 0;
};

class ParallelMetadataPocTest : public MetadataTableTestBase {
 protected:
  void SetUp() override {
    MetadataTableTestBase::SetUp();
    avro::RegisterAll();
    ICEBERG_UNWRAP_OR_FAIL(spec_, PartitionSpec::Make(0, {}));
    metadata_->partition_specs = {spec_};
    metadata_->default_spec_id = 0;
    for (int index = 0; index < 4; ++index) {
      const auto path = "manifest-" + std::to_string(index) + ".avro";
      ICEBERG_UNWRAP_OR_FAIL(
          auto writer,
          ManifestWriter::MakeWriter(2, 100, path, io_, spec_, metadata_->schemas[0],
                                     ManifestContent::kData));
      ASSERT_THAT(writer->WriteAddedEntry(File("added-" + std::to_string(index)), 7),
                  IsOk());
      ASSERT_THAT(writer->WriteExistingEntry(File("existing-" + std::to_string(index)),
                                             100, 6, 6),
                  IsOk());
      ASSERT_THAT(
          writer->WriteDeletedEntry(File("deleted-" + std::to_string(index)), 5, 5),
          IsOk());
      ASSERT_THAT(writer->Close(), IsOk());
      ICEBERG_UNWRAP_OR_FAIL(auto manifest, writer->ToManifestFile());
      manifests_.push_back(std::move(manifest));
    }
    ICEBERG_UNWRAP_OR_FAIL(auto list,
                           ManifestListWriter::MakeWriter(2, 100, std::nullopt,
                                                          "manifest-list.avro", io_, 7));
    ASSERT_THAT(list->AddAll(manifests_), IsOk());
    ASSERT_THAT(list->Close(), IsOk());
    metadata_->snapshots = {
        std::make_shared<Snapshot>(Snapshot{.snapshot_id = 100,
                                            .sequence_number = 7,
                                            .timestamp_ms = TimePointMsFromUnixMs(1000),
                                            .manifest_list = "manifest-list.avro",
                                            .schema_id = metadata_->current_schema_id})};
    metadata_->current_snapshot_id = 100;
  }

  static std::shared_ptr<DataFile> File(std::string path) {
    return std::make_shared<DataFile>(DataFile{
        .file_path = std::move(path), .record_count = 10, .file_size_in_bytes = 100});
  }

  MetadataScanContext Context(std::shared_ptr<Schema> projection = nullptr) {
    return {.kind = MetadataTableKind::kDataFiles,
            .table_metadata = metadata_,
            .schema = ManifestMetadataSchema(),
            .projected_schema =
                projection ? std::move(projection) : ManifestMetadataSchema()};
  }

  std::shared_ptr<MockFileIO> WorkerStorage() {
    auto storage = std::make_shared<MockFileIO>();
    for (const auto& manifest : manifests_) {
      storage->AddFile(manifest.manifest_path, io_->FileData(manifest.manifest_path));
    }
    // No manifest list or source-table object is available to the worker.
    return storage;
  }

  std::shared_ptr<PartitionSpec> spec_;
  std::vector<ManifestFile> manifests_;
};

TEST_F(ParallelMetadataPocTest, ParallelRoundTripMatchesSequentialAndOverlapsReads) {
  ICEBERG_UNWRAP_OR_FAIL(auto scan, PlanManifestMetadataScan(Context(), io_));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanTasks());
  ASSERT_EQ(tasks.size(), 4);
  ICEBERG_UNWRAP_OR_FAIL(auto sequential_stream, scan->ToArrowArrayStream());
  ICEBERG_UNWRAP_OR_FAIL(auto expected, ReadArrowTable(std::move(sequential_stream)));
  ASSERT_EQ(expected->num_rows(), 8);

  auto worker_io = std::make_shared<ProbedFileIO>(WorkerStorage(), true);
  std::vector<std::string> payloads;
  for (const auto& task : tasks) {
    const auto& manifest_task = dynamic_cast<const ManifestMetadataTask&>(*task);
    ICEBERG_UNWRAP_OR_FAIL(auto json, SerializeManifestMetadataTask(manifest_task));
    payloads.push_back(std::move(json));
  }
  std::weak_ptr<FileIO> driver_io = io_;
  tasks.clear();
  scan.reset();
  table_.reset();
  metadata_.reset();
  io_.reset();
  EXPECT_TRUE(driver_io.expired());

  for (const auto& json : payloads) {
    ICEBERG_UNWRAP_OR_FAIL(auto task, DeserializeManifestMetadataTask(json, worker_io));
    EXPECT_EQ(task->descriptor().snapshot_id, 100);
    tasks.push_back(std::move(task));
  }
  test::ThreadExecutor executor;
  ICEBERG_UNWRAP_OR_FAIL(
      auto parallel_stream,
      ExecuteManifestMetadataTasks(
          ManifestMetadataSchema(), tasks,
          {.executor = &executor, .max_tasks_in_flight = 2, .batch_size = 1}));
  ICEBERG_UNWRAP_OR_FAIL(auto actual, ReadArrowTable(std::move(parallel_stream)));
  EXPECT_TRUE(TablesEqual(*expected, *actual));
  EXPECT_EQ(worker_io->open_count(), 4);
  EXPECT_EQ(worker_io->peak(), 2);
  EXPECT_EQ(executor.submit_count(), 4);
}

TEST_F(ParallelMetadataPocTest, PlanningAssignsEachManifestOnceAndPinsSnapshot) {
  auto context = Context();
  context.snapshot_id = 100;
  metadata_->snapshots.push_back(
      std::make_shared<Snapshot>(Snapshot{.snapshot_id = 200}));
  metadata_->current_snapshot_id = 200;
  ICEBERG_UNWRAP_OR_FAIL(auto scan, PlanManifestMetadataScan(context, io_));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanTasks());
  ASSERT_EQ(tasks.size(), 4);
  std::set<std::string> paths;
  for (const auto& task : tasks) {
    const auto& descriptor =
        dynamic_cast<const ManifestMetadataTask&>(*task).descriptor();
    EXPECT_EQ(descriptor.snapshot_id, 100);
    EXPECT_TRUE(paths.insert(descriptor.manifest.manifest_path).second);
  }
  ICEBERG_UNWRAP_OR_FAIL(auto stream, scan->ToArrowArrayStream());
  ICEBERG_UNWRAP_OR_FAIL(auto rows, ReadArrowTable(std::move(stream)));
  EXPECT_EQ(rows->num_rows(), 8);
}

TEST_F(ParallelMetadataPocTest, ProjectionAndBatchSizeSurviveSerialization) {
  auto projection = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(4, "bytes", int64()),
                               SchemaField::MakeRequired(2, "path", string())});
  ICEBERG_UNWRAP_OR_FAIL(auto scan, PlanManifestMetadataScan(Context(projection), io_));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanTasks());
  const auto& original = dynamic_cast<const ManifestMetadataTask&>(*tasks[0]);
  ICEBERG_UNWRAP_OR_FAIL(auto payload, SerializeManifestMetadataTask(original));
  ICEBERG_UNWRAP_OR_FAIL(auto restored, DeserializeManifestMetadataTask(payload, io_));
  ICEBERG_UNWRAP_OR_FAIL(auto reader, MetadataScanTaskReader::Make({.batch_size = 1}));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, reader->Open(*restored));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 2);
  EXPECT_EQ(batches[0]->num_rows(), 1);
  EXPECT_EQ(batches[0]->schema()->field_names(),
            (std::vector<std::string>{"bytes", "path"}));
  EXPECT_EQ(
      std::static_pointer_cast<::arrow::Int64Array>(batches[0]->column(0))->Value(0),
      100);
}

TEST_F(ParallelMetadataPocTest, EmptySnapshotProducesAnEmptyStream) {
  metadata_->current_snapshot_id = kInvalidSnapshotId;
  metadata_->snapshots.clear();
  ICEBERG_UNWRAP_OR_FAIL(auto scan, PlanManifestMetadataScan(Context(), io_));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanTasks());
  EXPECT_TRUE(tasks.empty());
  ICEBERG_UNWRAP_OR_FAIL(auto stream,
                         ExecuteManifestMetadataTasks(ManifestMetadataSchema(), tasks));
  ICEBERG_UNWRAP_OR_FAIL(auto actual, ReadArrowTable(std::move(stream)));
  EXPECT_EQ(actual->num_rows(), 0);
  EXPECT_EQ(actual->num_columns(), 4);
}

TEST_F(ParallelMetadataPocTest, RejectsMalformedOrUnsupportedTransport) {
  ICEBERG_UNWRAP_OR_FAIL(auto scan, PlanManifestMetadataScan(Context(), io_));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanTasks());
  const auto& task = dynamic_cast<const ManifestMetadataTask&>(*tasks[0]);
  ICEBERG_UNWRAP_OR_FAIL(auto payload, SerializeManifestMetadataTask(task));
  auto json = nlohmann::json::parse(payload);
  json["version"] = 2;
  EXPECT_THAT(DeserializeManifestMetadataTask(json.dump(), io_),
              IsError(ErrorKind::kNotSupported));
  json = nlohmann::json::parse(payload);
  json["snapshot-id"] = 1.5;
  EXPECT_FALSE(DeserializeManifestMetadataTask(json.dump(), io_).has_value());
  json = nlohmann::json::parse(payload);
  json["manifest"]["spec-id"] = int64_t{1} << 40;
  EXPECT_FALSE(DeserializeManifestMetadataTask(json.dump(), io_).has_value());
  json = nlohmann::json::parse(payload);
  json.erase("source-schema");
  EXPECT_FALSE(DeserializeManifestMetadataTask(json.dump(), io_).has_value());
  EXPECT_FALSE(DeserializeManifestMetadataTask("{", io_).has_value());
  EXPECT_THAT(DeserializeManifestMetadataTask(payload, nullptr),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_F(ParallelMetadataPocTest, ReadFailureDrainsTheActiveWindowAndStopsSubmission) {
  ICEBERG_UNWRAP_OR_FAIL(auto scan, PlanManifestMetadataScan(Context(), io_));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanTasks());
  auto worker_io = std::make_shared<ProbedFileIO>(WorkerStorage(), false);
  for (size_t index = 0; index < tasks.size(); ++index) {
    auto descriptor =
        dynamic_cast<const ManifestMetadataTask&>(*tasks[index]).descriptor();
    if (index == 0) {
      descriptor.manifest.manifest_path = "missing.avro";
    }
    ICEBERG_UNWRAP_OR_FAIL(auto worker_task,
                           ManifestMetadataTask::Make(std::move(descriptor), worker_io));
    tasks[index] = std::move(worker_task);
  }
  test::ThreadExecutor executor;
  auto result = ExecuteManifestMetadataTasks(
      ManifestMetadataSchema(), tasks,
      {.executor = &executor, .max_tasks_in_flight = 2, .batch_size = 1});
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(executor.submit_count(), 2);
  EXPECT_EQ(worker_io->open_count(), 2);
}

TEST_F(ParallelMetadataPocTest, RejectsInvalidExecutionInputs) {
  ICEBERG_UNWRAP_OR_FAIL(auto scan, PlanManifestMetadataScan(Context(), io_));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanTasks());
  EXPECT_THAT(ExecuteManifestMetadataTasks(ManifestMetadataSchema(), tasks,
                                           {.max_tasks_in_flight = 0}),
              IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(
      ExecuteManifestMetadataTasks(ManifestMetadataSchema(), tasks, {.batch_size = 0}),
      IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(ExecuteManifestMetadataTasks(ManifestMetadataSchema(), {nullptr}),
              IsError(ErrorKind::kInvalidArgument));
  auto other_schema = std::make_shared<Schema>(std::vector<SchemaField>{});
  EXPECT_THAT(ExecuteManifestMetadataTasks(other_schema, tasks),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_F(ParallelMetadataPocTest, ExecutorRejectionIsReported) {
  ICEBERG_UNWRAP_OR_FAIL(auto scan, PlanManifestMetadataScan(Context(), io_));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanTasks());
  test::ThreadExecutor executor(IOError("Executor rejected task"));
  EXPECT_THAT(
      ExecuteManifestMetadataTasks(ManifestMetadataSchema(), tasks,
                                   {.executor = &executor, .max_tasks_in_flight = 2}),
      IsError(ErrorKind::kIOError));
  EXPECT_EQ(executor.submit_count(), 2);
}

}  // namespace
}  // namespace iceberg::internal
