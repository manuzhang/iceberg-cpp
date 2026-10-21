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

#include "iceberg/inspect/metadata_table_scan.h"

#include <memory>
#include <tuple>
#include <utility>

#include "iceberg/inspect/metadata_table.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/snapshot_util.h"
#include "iceberg/util/timepoint.h"

namespace iceberg {

MetadataTableScan::~MetadataTableScan() = default;

MetadataTableScanBuilder::MetadataTableScanBuilder(
    const MetadataTable& table, std::shared_ptr<TableMetadata> metadata,
    std::shared_ptr<FileIO> io)
    : table_(table), metadata_(std::move(metadata)), io_(std::move(io)) {}

MetadataTableScanBuilder& MetadataTableScanBuilder::Select(
    const std::vector<std::string>& column_names) {
  context_.selected_columns = column_names;
  return *this;
}

MetadataTableScanBuilder& MetadataTableScanBuilder::CaseSensitive(bool case_sensitive) {
  context_.case_sensitive = case_sensitive;
  return *this;
}

MetadataTableScanBuilder& MetadataTableScanBuilder::UseSnapshot(int64_t snapshot_id) {
  ICEBERG_BUILDER_CHECK(
      table_.supports_time_travel(),
      "Cannot select snapshot {}: metadata table does not support time travel",
      snapshot_id);
  ICEBERG_BUILDER_CHECK(!context_.snapshot_id.has_value(),
                        "Cannot override snapshot, already set snapshot id={}",
                        context_.snapshot_id.value());
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(std::ignore, metadata_->SnapshotById(snapshot_id));
  context_.snapshot_id = snapshot_id;
  return *this;
}

MetadataTableScanBuilder& MetadataTableScanBuilder::UseRef(const std::string& ref) {
  if (ref == SnapshotRef::kMainBranch) {
    context_.snapshot_id.reset();
    return *this;
  }
  ICEBERG_BUILDER_CHECK(
      table_.supports_time_travel(),
      "Cannot select ref {}: metadata table does not support time travel", ref);
  ICEBERG_BUILDER_CHECK(!context_.snapshot_id.has_value(),
                        "Cannot override ref, already set snapshot id={}",
                        context_.snapshot_id.value());
  auto iter = metadata_->refs.find(ref);
  ICEBERG_BUILDER_CHECK(iter != metadata_->refs.end(), "Cannot find ref {}", ref);
  ICEBERG_BUILDER_CHECK(iter->second != nullptr, "Ref {} is null", ref);
  const int64_t snapshot_id = iter->second->snapshot_id;
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(std::ignore, metadata_->SnapshotById(snapshot_id));
  context_.snapshot_id = snapshot_id;
  return *this;
}

MetadataTableScanBuilder& MetadataTableScanBuilder::AsOfTime(int64_t timestamp_millis) {
  ICEBERG_BUILDER_CHECK(
      table_.supports_time_travel(),
      "Cannot select a snapshot as of {}: metadata table does not support time travel",
      timestamp_millis);
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(
      auto snapshot_id, SnapshotUtil::SnapshotIdAsOfTime(
                            *metadata_, TimePointMsFromUnixMs(timestamp_millis)));
  return UseSnapshot(snapshot_id);
}

Result<std::unique_ptr<MetadataTableScan>> MetadataTableScanBuilder::Build() {
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());
  ICEBERG_RETURN_UNEXPECTED(context_.Validate());
  return table_.MakeScan(metadata_, io_, std::move(context_));
}

}  // namespace iceberg
