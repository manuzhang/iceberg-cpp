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

/// \file iceberg/inspect/metadata_table_scan.h
/// \brief Scan APIs for Iceberg metadata tables.

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/table_scan.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/error_collector.h"

namespace iceberg {

/// \brief A configured scan of a metadata table.
///
/// The scan holds the source table metadata captured by MetadataTable::NewScan(), so
/// planning and reading see the same metadata even if the source table refreshes.
/// TableScan::schema() returns the projected schema that every planned task uses.
class ICEBERG_EXPORT MetadataTableScan : public TableScan {
 public:
  ~MetadataTableScan() override;

  /// \brief Plan the tasks that produce this scan's rows.
  ///
  /// Each task returns rows in schema(). Tasks can be read independently, for example
  /// one per manifest for metadata tables backed by manifests.
  virtual Result<std::vector<std::shared_ptr<DataTask>>> PlanFiles() const = 0;

 protected:
  using TableScan::TableScan;
};

/// \brief Builder for metadata table scans.
///
/// Create one with MetadataTable::NewScan(). The metadata table must outlive the
/// builder; scans built from it do not reference the metadata table.
class ICEBERG_EXPORT MetadataTableScanBuilder : public ErrorCollector {
 public:
  /// \brief Read only the given columns.
  ///
  /// Names resolve as in Schema::Select. If this method is not called, all columns are
  /// selected.
  ///
  /// \param column_names Column names from the metadata table's schema.
  MetadataTableScanBuilder& Select(const std::vector<std::string>& column_names);

  /// \brief Set whether Select() matches column names case-sensitively. Defaults to
  /// true.
  MetadataTableScanBuilder& CaseSensitive(bool case_sensitive);

  /// \brief Read the metadata of the given snapshot.
  ///
  /// Only metadata tables that support time travel accept a snapshot.
  ///
  /// \param snapshot_id A snapshot ID of the source table.
  MetadataTableScanBuilder& UseSnapshot(int64_t snapshot_id);

  /// \brief Read the metadata of the snapshot that a branch or tag references.
  ///
  /// The main branch is always accepted and selects the current snapshot. Other refs
  /// require a metadata table that supports time travel.
  ///
  /// \param ref A branch or tag name of the source table.
  MetadataTableScanBuilder& UseRef(const std::string& ref);

  /// \brief Read the metadata of the most recent snapshot as of the given time.
  ///
  /// Only metadata tables that support time travel accept a timestamp.
  ///
  /// \param timestamp_millis Milliseconds since the Unix epoch.
  MetadataTableScanBuilder& AsOfTime(int64_t timestamp_millis);

  /// \brief Build the configured scan.
  Result<std::unique_ptr<MetadataTableScan>> Build();

 private:
  friend class MetadataTable;

  MetadataTableScanBuilder(const MetadataTable& table,
                           std::shared_ptr<TableMetadata> metadata,
                           std::shared_ptr<FileIO> io);

  const MetadataTable& table_;
  std::shared_ptr<TableMetadata> metadata_;
  std::shared_ptr<FileIO> io_;
  internal::TableScanContext context_;
};

}  // namespace iceberg
