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

#include "iceberg/inspect/snapshots_table.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "iceberg/inspect/static_data_task_internal.h"
#include "iceberg/row/struct_like.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg {
namespace {

// A snapshot summary without the operation entry, which has its own column.
class SummaryMapLike : public MapLike {
 public:
  explicit SummaryMapLike(std::shared_ptr<Snapshot> snapshot)
      : snapshot_(std::move(snapshot)) {
    entries_.reserve(snapshot_->summary.size());
    for (const auto& entry : snapshot_->summary) {
      if (entry.first != SnapshotSummaryFields::kOperation) {
        entries_.push_back(&entry);
      }
    }
  }

  Result<Scalar> GetKey(size_t pos) const override {
    if (pos >= entries_.size()) {
      return InvalidArgument("Invalid map index: {}", pos);
    }
    return std::string_view(entries_[pos]->first);
  }

  Result<Scalar> GetValue(size_t pos) const override {
    if (pos >= entries_.size()) {
      return InvalidArgument("Invalid map index: {}", pos);
    }
    return std::string_view(entries_[pos]->second);
  }

  size_t size() const override { return entries_.size(); }

 private:
  std::shared_ptr<Snapshot> snapshot_;
  std::vector<const std::pair<const std::string, std::string>*> entries_;
};

// A snapshot viewed as a row of the snapshots table.
class SnapshotRow : public StructLike {
 public:
  explicit SnapshotRow(std::shared_ptr<Snapshot> snapshot)
      : snapshot_(std::move(snapshot)) {}

  Result<Scalar> GetField(size_t pos) const override {
    switch (pos) {
      case 0:
        return static_cast<int64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                        snapshot_->timestamp_ms.time_since_epoch())
                                        .count());
      case 1:
        return snapshot_->snapshot_id;
      case 2:
        if (snapshot_->parent_snapshot_id.has_value()) {
          return *snapshot_->parent_snapshot_id;
        }
        return std::monostate{};
      case 3:
        if (auto operation = snapshot_->Operation(); operation.has_value()) {
          return *operation;
        }
        return std::monostate{};
      case 4:
        return std::string_view(snapshot_->manifest_list);
      case 5:
        // A missing summary is null. A summary that held only the operation is an
        // empty map.
        if (snapshot_->summary.empty()) {
          return std::monostate{};
        }
        return std::make_shared<SummaryMapLike>(snapshot_);
      default:
        return InvalidArgument("Invalid snapshots table field index: {}", pos);
    }
  }

  size_t num_fields() const override { return 6; }

 private:
  std::shared_ptr<Snapshot> snapshot_;
};

class SnapshotsTableScan : public MetadataTableScan {
 public:
  SnapshotsTableScan(std::shared_ptr<TableMetadata> metadata,
                     std::shared_ptr<Schema> schema, std::shared_ptr<FileIO> io,
                     internal::TableScanContext context)
      : MetadataTableScan(std::move(metadata), std::move(schema), std::move(io),
                          std::move(context)) {}

  Result<std::vector<std::shared_ptr<DataTask>>> PlanFiles() const override {
    ICEBERG_ASSIGN_OR_RAISE(auto projected_schema, schema());
    std::vector<std::shared_ptr<StructLike>> rows;
    rows.reserve(metadata_->snapshots.size());
    for (const auto& snapshot : metadata_->snapshots) {
      if (snapshot == nullptr) [[unlikely]] {
        continue;
      }
      rows.push_back(std::make_shared<SnapshotRow>(snapshot));
    }
    ICEBERG_ASSIGN_OR_RAISE(
        auto task,
        StaticDataTask::Make(*schema_, std::move(projected_schema), std::move(rows)));
    return std::vector<std::shared_ptr<DataTask>>{std::move(task)};
  }
};

}  // namespace

SnapshotsTable::SnapshotsTable(std::shared_ptr<Table> table)
    : MetadataTable(std::move(table)) {}

SnapshotsTable::~SnapshotsTable() = default;

const std::shared_ptr<Schema>& SnapshotsTable::schema() const {
  static const auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "committed_at", timestamp_tz()),
      SchemaField::MakeRequired(2, "snapshot_id", int64()),
      SchemaField::MakeOptional(3, "parent_id", int64()),
      SchemaField::MakeOptional(4, "operation", string()),
      SchemaField::MakeOptional(5, "manifest_list", string()),
      SchemaField::MakeOptional(6, "summary",
                                std::make_shared<iceberg::MapType>(
                                    SchemaField::MakeRequired(7, "key", string()),
                                    SchemaField::MakeRequired(8, "value", string())))});
  return schema;
}

Result<std::unique_ptr<SnapshotsTable>> SnapshotsTable::Make(
    std::shared_ptr<Table> table) {
  ICEBERG_PRECHECK(table != nullptr, "Table cannot be null");
  return std::unique_ptr<SnapshotsTable>(new SnapshotsTable(std::move(table)));
}

Result<std::unique_ptr<MetadataTableScan>> SnapshotsTable::MakeScan(
    std::shared_ptr<TableMetadata> metadata, std::shared_ptr<FileIO> io,
    internal::TableScanContext context) const {
  return std::make_unique<SnapshotsTableScan>(std::move(metadata), schema(),
                                              std::move(io), std::move(context));
}

}  // namespace iceberg
