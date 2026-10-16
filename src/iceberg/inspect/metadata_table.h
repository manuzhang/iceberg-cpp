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

/// \file iceberg/inspect/metadata_table.h
/// \brief Base APIs for inspecting Iceberg metadata tables.

#include <concepts>
#include <memory>
#include <utility>

#include "iceberg/iceberg_export.h"
#include "iceberg/inspect/metadata_table_scan.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Base interface for an Iceberg metadata table.
///
/// A metadata table exposes the source table's metadata as rows, read through a scan:
/// NewScan() configures the columns and, for tables that support time travel, the
/// snapshot; the scan plans DataTasks; and each task returns StructLike rows that a
/// caller consumes directly. ReadMetadataTable() reads a whole scan as Arrow batches.
class ICEBERG_EXPORT MetadataTable {
 public:
  /// \brief Supported metadata table kinds.
  enum class Kind {
    kSnapshots,
    kHistory,
  };

  /// \brief Create a metadata table of the requested concrete type.
  ///
  /// \tparam MetadataTableType Concrete class derived from MetadataTable.
  /// \param table Source table whose metadata will be exposed.
  /// \return The constructed metadata table, or an error.
  template <typename MetadataTableType>
    requires std::derived_from<MetadataTableType, MetadataTable>
  static Result<std::unique_ptr<MetadataTableType>> Make(std::shared_ptr<Table> table) {
    return MetadataTableType::Make(std::move(table));
  }

  virtual ~MetadataTable();

  /// \brief Return this metadata table's kind.
  virtual Kind kind() const noexcept = 0;

  /// \brief Return the schema of rows emitted by scans.
  virtual const std::shared_ptr<Schema>& schema() const = 0;

  /// \brief Return the source table whose metadata is exposed.
  const std::shared_ptr<Table>& source_table() const;

  /// \brief Return whether scans of this table accept a snapshot selection.
  virtual bool supports_time_travel() const noexcept;

  /// \brief Create a builder for a scan of this metadata table.
  ///
  /// The builder captures the source table's current metadata. This metadata table must
  /// outlive the builder.
  std::unique_ptr<MetadataTableScanBuilder> NewScan() const;

 protected:
  explicit MetadataTable(std::shared_ptr<Table> source_table);

 private:
  friend class MetadataTableScanBuilder;

  /// \brief Create a scan of this table.
  ///
  /// \param metadata Source table metadata captured by NewScan().
  /// \param io FileIO of the source table.
  /// \param context Scan options validated by the builder.
  virtual Result<std::unique_ptr<MetadataTableScan>> MakeScan(
      std::shared_ptr<TableMetadata> metadata, std::shared_ptr<FileIO> io,
      internal::TableScanContext context) const = 0;

  std::shared_ptr<Table> source_table_;
};

}  // namespace iceberg
