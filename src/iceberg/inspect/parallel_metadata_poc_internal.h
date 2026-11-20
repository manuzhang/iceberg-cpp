/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "iceberg/arrow_c_data.h"
#include "iceberg/constants.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/inspect/metadata_scan.h"
#include "iceberg/inspect/metadata_scan_task.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg::internal {

/// Experimental, non-installed API demonstrating manifest-sized metadata tasks.
/// The output columns are manifest_path, file_path, record_count, file_size_in_bytes.
ICEBERG_EXPORT std::shared_ptr<Schema> ManifestMetadataSchema();

/// A fixed work assignment. Schema/spec/projection objects must remain immutable.
/// Runtime FileIO and reader callbacks are deliberately absent from the descriptor.
struct ManifestMetadataTaskDescriptor {
  int64_t snapshot_id = kInvalidSnapshotId;
  ManifestFile manifest;
  std::shared_ptr<Schema> source_schema;
  std::shared_ptr<PartitionSpec> partition_spec;
  std::shared_ptr<Schema> projected_schema;
};

class ICEBERG_EXPORT ManifestMetadataTask final : public MetadataScanTask {
 public:
  static Result<std::shared_ptr<ManifestMetadataTask>> Make(
      ManifestMetadataTaskDescriptor descriptor, std::shared_ptr<FileIO> io);

  TaskKind task_kind() const override { return TaskKind::kManifestEntries; }
  MetadataTableKind table_kind() const override;

  const ManifestMetadataTaskDescriptor& descriptor() const { return descriptor_; }
  Result<ArrowArrayStream> Open(int64_t batch_size) const;

 private:
  ManifestMetadataTask(ManifestMetadataTaskDescriptor descriptor,
                       std::shared_ptr<FileIO> io);

  const ManifestMetadataTaskDescriptor descriptor_;
  const std::shared_ptr<FileIO> io_;
};

/// Resolve a snapshot once, then plan one disjoint task per data manifest.
ICEBERG_EXPORT Result<std::unique_ptr<MetadataScan>> PlanManifestMetadataScan(
    MetadataScanContext context, std::shared_ptr<FileIO> io);

/// Versioned JSON with the work assignment, source schema/spec and projection.
ICEBERG_EXPORT Result<std::string> SerializeManifestMetadataTask(
    const ManifestMetadataTask& task);

/// Workers reconstruct a task with their own execution FileIO.
ICEBERG_EXPORT Result<std::shared_ptr<ManifestMetadataTask>>
DeserializeManifestMetadataTask(std::string_view json, std::shared_ptr<FileIO> io);

struct ParallelMetadataReadOptions {
  /// Non-owning; the executor must make progress until this synchronous call ends.
  Executor* executor = nullptr;
  size_t max_tasks_in_flight = 4;
  int64_t batch_size = 1024;
};

/// Consume independent task streams in bounded parallel windows, preserving task
/// order. This PoC eagerly buffers every output batch before returning. Streaming
/// backpressure, cancellation and partition-stat reduction require further work.
/// An error drains the active window and releases all buffered partial results.
ICEBERG_EXPORT Result<ArrowArrayStream> ExecuteManifestMetadataTasks(
    std::shared_ptr<Schema> projected_schema,
    std::vector<std::shared_ptr<MetadataScanTask>> tasks,
    ParallelMetadataReadOptions options = {});

}  // namespace iceberg::internal
