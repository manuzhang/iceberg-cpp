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

#include <algorithm>
#include <array>
#include <optional>
#include <utility>

#include <nanoarrow/nanoarrow.hpp>
#include <nlohmann/json.hpp>

#include "iceberg/arrow_c_data_util_internal.h"
#include "iceberg/arrow_row_builder_internal.h"
#include "iceberg/inspect/metadata_scan_task_reader.h"
#include "iceberg/inspect/metadata_scan_util_internal.h"
#include "iceberg/inspect/metadata_table.h"
#include "iceberg/inspect/metadata_table_util_internal.h"
#include "iceberg/inspect/parallel_metadata_poc_internal.h"
#include "iceberg/json_serde_internal.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/schema_internal.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/type.h"
#include "iceberg/util/json_util_internal.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/task_group.h"

namespace iceberg::internal {
namespace {

struct FileRow {
  std::string manifest_path;
  std::string file_path;
  int64_t record_count;
  int64_t file_size_in_bytes;
};

constexpr std::array<MetadataTableColumn<FileRow>, 4> kColumns = {{
    {.field_id = 1,
     .append = [](ArrowArray* array,
                  const FileRow& row) { return AppendString(array, row.manifest_path); }},
    {.field_id = 2,
     .append = [](ArrowArray* array,
                  const FileRow& row) { return AppendString(array, row.file_path); }},
    {.field_id = 3,
     .append = [](ArrowArray* array,
                  const FileRow& row) { return AppendInt(array, row.record_count); }},
    {.field_id = 4,
     .append =
         [](ArrowArray* array, const FileRow& row) {
           return AppendInt(array, row.file_size_in_bytes);
         }},
}};

class ManifestMetadataScan : public MetadataScan {
 public:
  ManifestMetadataScan(MetadataScanContext context,
                       std::vector<std::shared_ptr<MetadataScanTask>> tasks)
      : MetadataScan(std::move(context)), tasks_(std::move(tasks)) {}

  Result<MetadataScanTaskStreamPtr> PlanTasksStream() const override {
    return MetadataScanTaskStream::FromVector(tasks_);
  }

 private:
  const std::vector<std::shared_ptr<MetadataScanTask>> tasks_;
};

using Batches = std::vector<std::vector<nanoarrow::UniqueArray>>;

class CollectedBatches {
 public:
  CollectedBatches(std::shared_ptr<iceberg::Schema> schema, Batches batches)
      : schema_(std::move(schema)), batches_(std::move(batches)) {}

  Status Close() {
    batches_.clear();
    return {};
  }

  Result<ArrowSchema> Schema() {
    ArrowSchema out{};
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(*schema_, &out));
    return out;
  }

  Result<std::optional<ArrowArray>> Next() {
    while (task_index_ < batches_.size()) {
      auto& task_batches = batches_[task_index_];
      if (batch_index_ < task_batches.size()) {
        ArrowArray out{};
        task_batches[batch_index_++].move(&out);
        return out;
      }
      ++task_index_;
      batch_index_ = 0;
    }
    return std::nullopt;
  }

 private:
  std::shared_ptr<iceberg::Schema> schema_;
  Batches batches_;
  size_t task_index_ = 0;
  size_t batch_index_ = 0;
};

Status CollectTask(const MetadataScanTask& task, int64_t batch_size,
                   std::vector<nanoarrow::UniqueArray>& batches) {
  ICEBERG_ASSIGN_OR_RAISE(auto reader,
                          MetadataScanTaskReader::Make({.batch_size = batch_size}));
  ICEBERG_ASSIGN_OR_RAISE(auto stream, reader->Open(task));
  nanoarrow::UniqueArrayStream owned(&stream);
  while (true) {
    nanoarrow::UniqueArray array;
    if (owned->get_next(owned.get(), array.get()) != 0) {
      const char* message =
          owned->get_last_error != nullptr ? owned->get_last_error(owned.get()) : nullptr;
      return IOError("{}", message != nullptr ? message : "Metadata task read failed");
    }
    if (array->release == nullptr) {
      return {};
    }
    batches.push_back(std::move(array));
  }
}

}  // namespace

std::shared_ptr<Schema> ManifestMetadataSchema() {
  static const auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "manifest_path", string()),
      SchemaField::MakeRequired(2, "file_path", string()),
      SchemaField::MakeRequired(3, "record_count", int64()),
      SchemaField::MakeRequired(4, "file_size_in_bytes", int64())});
  return schema;
}

Result<std::shared_ptr<ManifestMetadataTask>> ManifestMetadataTask::Make(
    ManifestMetadataTaskDescriptor descriptor, std::shared_ptr<FileIO> io) {
  ICEBERG_PRECHECK(io != nullptr, "Worker FileIO cannot be null");
  ICEBERG_PRECHECK(descriptor.snapshot_id >= 0, "Snapshot ID must be non-negative");
  ICEBERG_PRECHECK(!descriptor.manifest.manifest_path.empty() &&
                       descriptor.manifest.manifest_length > 0,
                   "Manifest path and positive length are required");
  ICEBERG_PRECHECK(descriptor.manifest.added_snapshot_id.has_value(),
                   "Manifest must belong to a committed snapshot");
  ICEBERG_PRECHECK(descriptor.manifest.sequence_number >= 0,
                   "Manifest sequence number must be non-negative");
  if (descriptor.manifest.content != ManifestContent::kData ||
      !descriptor.manifest.key_metadata.empty()) {
    return NotSupported("PoC supports unencrypted data manifests only");
  }
  ICEBERG_PRECHECK(descriptor.source_schema != nullptr &&
                       descriptor.partition_spec != nullptr &&
                       descriptor.projected_schema != nullptr,
                   "Task schemas and partition spec cannot be null");
  ICEBERG_PRECHECK(
      descriptor.manifest.partition_spec_id == descriptor.partition_spec->spec_id(),
      "Task partition spec does not match the manifest");
  ICEBERG_RETURN_UNEXPECTED(
      descriptor.partition_spec->RawPartitionType(*descriptor.source_schema));
  ICEBERG_RETURN_UNEXPECTED(SelectAppendFns<FileRow>(
      *ManifestMetadataSchema(), *descriptor.projected_schema, kColumns));
  return std::shared_ptr<ManifestMetadataTask>(
      new ManifestMetadataTask(std::move(descriptor), std::move(io)));
}

ManifestMetadataTask::ManifestMetadataTask(ManifestMetadataTaskDescriptor descriptor,
                                           std::shared_ptr<FileIO> io)
    : descriptor_(std::move(descriptor)), io_(std::move(io)) {}

MetadataTableKind ManifestMetadataTask::table_kind() const {
  return MetadataTableKind::kDataFiles;
}

Result<ArrowArrayStream> ManifestMetadataTask::Open(int64_t batch_size) const {
  ICEBERG_PRECHECK(batch_size > 0, "Batch size must be positive");
  ICEBERG_ASSIGN_OR_RAISE(auto reader, ManifestReader::Make(descriptor_.manifest, io_,
                                                            descriptor_.source_schema,
                                                            descriptor_.partition_spec));
  ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->LiveEntries());
  auto rows = std::make_shared<std::vector<FileRow>>();
  rows->reserve(entries.size());
  for (const auto& entry : entries) {
    ICEBERG_CHECK(entry.data_file != nullptr, "Live entry has no data file");
    rows->push_back({.manifest_path = descriptor_.manifest.manifest_path,
                     .file_path = entry.data_file->file_path,
                     .record_count = entry.data_file->record_count,
                     .file_size_in_bytes = entry.data_file->file_size_in_bytes});
  }
  return MakeMetadataBatchStream<FileRow>(
      ManifestMetadataSchema(), descriptor_.projected_schema, kColumns, rows->size(),
      batch_size, [rows](size_t index) -> const FileRow& { return (*rows)[index]; });
}

Result<std::unique_ptr<MetadataScan>> PlanManifestMetadataScan(
    MetadataScanContext context, std::shared_ptr<FileIO> io) {
  ICEBERG_RETURN_UNEXPECTED(context.Validate());
  ICEBERG_PRECHECK(context.kind == MetadataTableKind::kDataFiles,
                   "PoC plans data-file metadata only");
  ICEBERG_PRECHECK(io != nullptr, "Planning FileIO cannot be null");
  ICEBERG_ASSIGN_OR_RAISE(auto snapshot,
                          ResolveMetadataTableSnapshot(*context.table_metadata, context));
  std::vector<std::shared_ptr<MetadataScanTask>> tasks;
  if (snapshot != nullptr) {
    ICEBERG_ASSIGN_OR_RAISE(auto schema,
                            snapshot->schema_id.has_value()
                                ? context.table_metadata->SchemaById(*snapshot->schema_id)
                                : context.table_metadata->Schema());
    SnapshotReader reader(snapshot.get());
    ICEBERG_ASSIGN_OR_RAISE(auto manifests, reader.DataManifests(io));
    tasks.reserve(manifests.size());
    for (const auto& manifest : manifests) {
      ICEBERG_ASSIGN_OR_RAISE(auto spec, context.table_metadata->PartitionSpecById(
                                             manifest.partition_spec_id));
      ICEBERG_ASSIGN_OR_RAISE(
          auto task,
          ManifestMetadataTask::Make({.snapshot_id = snapshot->snapshot_id,
                                      .manifest = manifest,
                                      .source_schema = schema,
                                      .partition_spec = std::move(spec),
                                      .projected_schema = context.projected_schema},
                                     io));
      tasks.push_back(std::move(task));
    }
  }
  return std::make_unique<ManifestMetadataScan>(std::move(context), std::move(tasks));
}

Result<std::string> SerializeManifestMetadataTask(const ManifestMetadataTask& task) {
  const auto& descriptor = task.descriptor();
  const auto& manifest = descriptor.manifest;
  ICEBERG_ASSIGN_OR_RAISE(auto schema, ToJson(*descriptor.source_schema));
  ICEBERG_ASSIGN_OR_RAISE(auto projection, ToJson(*descriptor.projected_schema));
  nlohmann::json manifest_json = {
      {"path", manifest.manifest_path},
      {"length", manifest.manifest_length},
      {"spec-id", manifest.partition_spec_id},
      {"sequence-number", manifest.sequence_number},
      {"min-sequence-number", manifest.min_sequence_number},
      {"added-snapshot-id", *manifest.added_snapshot_id},
  };
  if (manifest.first_row_id.has_value()) {
    manifest_json["first-row-id"] = *manifest.first_row_id;
  }
  nlohmann::json json = {{"version", 1},
                         {"task-type", "data-file-metadata-manifest"},
                         {"snapshot-id", descriptor.snapshot_id},
                         {"manifest", std::move(manifest_json)},
                         {"source-schema", std::move(schema)},
                         {"partition-spec", ToJson(*descriptor.partition_spec)},
                         {"projection", std::move(projection)}};
  try {
    return json.dump();
  } catch (const nlohmann::json::exception& error) {
    return JsonParseError("Cannot encode manifest task: {}", error.what());
  }
}

Result<std::shared_ptr<ManifestMetadataTask>> DeserializeManifestMetadataTask(
    std::string_view text, std::shared_ptr<FileIO> io) {
  ICEBERG_ASSIGN_OR_RAISE(auto json, ParseJsonString(text));
  ICEBERG_ASSIGN_OR_RAISE(auto version, GetJsonInteger<int32_t>(json, "version"));
  if (version != 1) {
    return NotSupported("Unsupported manifest task version: {}", version);
  }
  ICEBERG_ASSIGN_OR_RAISE(auto type, GetJsonValue<std::string>(json, "task-type"));
  if (type != "data-file-metadata-manifest") {
    return NotSupported("Unsupported manifest task type: {}", type);
  }
  ICEBERG_ASSIGN_OR_RAISE(auto snapshot_id, GetJsonInteger<int64_t>(json, "snapshot-id"));
  ICEBERG_ASSIGN_OR_RAISE(auto manifest_json,
                          GetJsonValue<nlohmann::json>(json, "manifest"));
  ManifestFile manifest;
  ICEBERG_ASSIGN_OR_RAISE(manifest.manifest_path,
                          GetJsonValue<std::string>(manifest_json, "path"));
  ICEBERG_ASSIGN_OR_RAISE(manifest.manifest_length,
                          GetJsonInteger<int64_t>(manifest_json, "length"));
  ICEBERG_ASSIGN_OR_RAISE(manifest.partition_spec_id,
                          GetJsonInteger<int32_t>(manifest_json, "spec-id"));
  ICEBERG_ASSIGN_OR_RAISE(manifest.sequence_number,
                          GetJsonInteger<int64_t>(manifest_json, "sequence-number"));
  ICEBERG_ASSIGN_OR_RAISE(manifest.min_sequence_number,
                          GetJsonInteger<int64_t>(manifest_json, "min-sequence-number"));
  ICEBERG_ASSIGN_OR_RAISE(auto added_snapshot,
                          GetJsonInteger<int64_t>(manifest_json, "added-snapshot-id"));
  manifest.added_snapshot_id = added_snapshot;
  if (manifest_json.contains("first-row-id")) {
    ICEBERG_ASSIGN_OR_RAISE(auto first_row,
                            GetJsonInteger<int64_t>(manifest_json, "first-row-id"));
    manifest.first_row_id = first_row;
  }
  ICEBERG_ASSIGN_OR_RAISE(auto schema_json,
                          GetJsonValue<nlohmann::json>(json, "source-schema"));
  ICEBERG_ASSIGN_OR_RAISE(auto schema, SchemaFromJson(schema_json));
  ICEBERG_ASSIGN_OR_RAISE(auto spec_json,
                          GetJsonValue<nlohmann::json>(json, "partition-spec"));
  ICEBERG_ASSIGN_OR_RAISE(auto spec, PartitionSpecFromJson(spec_json));
  ICEBERG_ASSIGN_OR_RAISE(auto projection_json,
                          GetJsonValue<nlohmann::json>(json, "projection"));
  ICEBERG_ASSIGN_OR_RAISE(auto projection, SchemaFromJson(projection_json));
  return ManifestMetadataTask::Make({.snapshot_id = snapshot_id,
                                     .manifest = std::move(manifest),
                                     .source_schema = std::move(schema),
                                     .partition_spec = std::move(spec),
                                     .projected_schema = std::move(projection)},
                                    std::move(io));
}

Result<ArrowArrayStream> ExecuteManifestMetadataTasks(
    std::shared_ptr<Schema> projected_schema,
    std::vector<std::shared_ptr<MetadataScanTask>> tasks,
    ParallelMetadataReadOptions options) {
  ICEBERG_PRECHECK(projected_schema != nullptr, "Output schema cannot be null");
  ICEBERG_PRECHECK(options.batch_size > 0 && options.max_tasks_in_flight > 0,
                   "Batch size and maximum in-flight task count must be positive");
  for (const auto& task : tasks) {
    const auto* manifest_task = dynamic_cast<const ManifestMetadataTask*>(task.get());
    ICEBERG_PRECHECK(manifest_task != nullptr, "Expected manifest metadata tasks");
    ICEBERG_PRECHECK(*manifest_task->descriptor().projected_schema == *projected_schema,
                     "All tasks must have the requested output schema");
  }
  Batches batches(tasks.size());
  for (size_t begin = 0; begin < tasks.size();) {
    const size_t count = std::min(options.max_tasks_in_flight, tasks.size() - begin);
    TaskGroup<> group;
    if (options.executor != nullptr) {
      group.SetExecutor(std::ref(*options.executor));
    }
    for (size_t index = begin; index < begin + count; ++index) {
      group.Submit([&, index]() {
        return CollectTask(*tasks[index], options.batch_size, batches[index]);
      });
    }
    ICEBERG_RETURN_UNEXPECTED(std::move(group).Run());
    begin += count;
  }
  return MakeArrowArrayStream(std::make_unique<CollectedBatches>(
      std::move(projected_schema), std::move(batches)));
}

}  // namespace iceberg::internal
