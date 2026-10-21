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

#include "iceberg/inspect/history_table.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "iceberg/constants.h"
#include "iceberg/inspect/static_data_task_internal.h"
#include "iceberg/row/struct_like.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/snapshot_util.h"

namespace iceberg {
namespace {

// A snapshot log entry viewed as a row of the history table.
class HistoryRow : public StructLike {
 public:
  HistoryRow(int64_t made_current_at_micros, int64_t snapshot_id,
             std::optional<int64_t> parent_id, bool is_current_ancestor)
      : made_current_at_micros_(made_current_at_micros),
        snapshot_id_(snapshot_id),
        parent_id_(parent_id),
        is_current_ancestor_(is_current_ancestor) {}

  Result<Scalar> GetField(size_t pos) const override {
    switch (pos) {
      case 0:
        return made_current_at_micros_;
      case 1:
        return snapshot_id_;
      case 2:
        if (parent_id_.has_value()) {
          return *parent_id_;
        }
        return std::monostate{};
      case 3:
        return is_current_ancestor_;
      default:
        return InvalidArgument("Invalid history table field index: {}", pos);
    }
  }

  size_t num_fields() const override { return 4; }

 private:
  int64_t made_current_at_micros_;
  int64_t snapshot_id_;
  std::optional<int64_t> parent_id_;
  bool is_current_ancestor_;
};

class HistoryTableScan : public MetadataTableScan {
 public:
  HistoryTableScan(std::shared_ptr<TableMetadata> metadata,
                   std::shared_ptr<Schema> schema, std::shared_ptr<FileIO> io,
                   internal::TableScanContext context)
      : MetadataTableScan(std::move(metadata), std::move(schema), std::move(io),
                          std::move(context)) {}

  Result<std::vector<std::shared_ptr<DataTask>>> PlanFiles() const override {
    ICEBERG_ASSIGN_OR_RAISE(auto projected_schema, schema());

    std::unordered_map<int64_t, std::optional<int64_t>> parent_ids;
    for (const auto& snapshot : metadata_->snapshots) {
      if (snapshot != nullptr) {
        parent_ids.emplace(snapshot->snapshot_id, snapshot->parent_snapshot_id);
      }
    }

    // A table without a current snapshot has no current ancestors.
    std::unordered_set<int64_t> ancestor_ids;
    if (metadata_->current_snapshot_id != kInvalidSnapshotId) {
      ICEBERG_ASSIGN_OR_RAISE(auto ancestors, SnapshotUtil::CurrentAncestors(*metadata_));
      for (const auto& ancestor : ancestors) {
        ancestor_ids.insert(ancestor->snapshot_id);
      }
    }

    std::vector<std::shared_ptr<StructLike>> rows;
    rows.reserve(metadata_->snapshot_log.size());
    for (const auto& entry : metadata_->snapshot_log) {
      std::optional<int64_t> parent_id;
      if (auto iter = parent_ids.find(entry.snapshot_id); iter != parent_ids.end()) {
        parent_id = iter->second;
      }
      rows.push_back(std::make_shared<HistoryRow>(
          static_cast<int64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                   entry.timestamp_ms.time_since_epoch())
                                   .count()),
          entry.snapshot_id, parent_id, ancestor_ids.contains(entry.snapshot_id)));
    }

    ICEBERG_ASSIGN_OR_RAISE(
        auto task,
        StaticDataTask::Make(*schema_, std::move(projected_schema), std::move(rows)));
    return std::vector<std::shared_ptr<DataTask>>{std::move(task)};
  }
};

}  // namespace

HistoryTable::HistoryTable(std::shared_ptr<Table> table)
    : MetadataTable(std::move(table)) {}

HistoryTable::~HistoryTable() = default;

const std::shared_ptr<Schema>& HistoryTable::schema() const {
  static const auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "made_current_at", timestamp_tz()),
      SchemaField::MakeRequired(2, "snapshot_id", int64()),
      SchemaField::MakeOptional(3, "parent_id", int64()),
      SchemaField::MakeRequired(4, "is_current_ancestor", boolean())});
  return schema;
}

Result<std::unique_ptr<HistoryTable>> HistoryTable::Make(std::shared_ptr<Table> table) {
  ICEBERG_PRECHECK(table != nullptr, "Table cannot be null");
  return std::unique_ptr<HistoryTable>(new HistoryTable(std::move(table)));
}

Result<std::unique_ptr<MetadataTableScan>> HistoryTable::MakeScan(
    std::shared_ptr<TableMetadata> metadata, std::shared_ptr<FileIO> io,
    internal::TableScanContext context) const {
  return std::make_unique<HistoryTableScan>(std::move(metadata), schema(), std::move(io),
                                            std::move(context));
}

}  // namespace iceberg
