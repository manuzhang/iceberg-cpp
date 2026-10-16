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

/// \file iceberg/inspect/history_table.h
/// \brief Define the history metadata table.

#include <memory>

#include "iceberg/iceberg_export.h"
#include "iceberg/inspect/metadata_table.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief History metadata table.
///
/// Scans return one row per snapshot log entry, in log order. The snapshot log records
/// each change to the table's current snapshot, so a snapshot appears again when a
/// rollback makes it current again. `is_current_ancestor` is true for the current
/// snapshot and its ancestors, and false for snapshots a rollback left behind. A log
/// entry whose snapshot is no longer in the metadata has a null `parent_id`.
class ICEBERG_EXPORT HistoryTable : public MetadataTable {
 public:
  static Result<std::unique_ptr<HistoryTable>> Make(std::shared_ptr<Table> table);

  ~HistoryTable() override;

  Kind kind() const noexcept override { return Kind::kHistory; }

  const std::shared_ptr<Schema>& schema() const override;

 private:
  explicit HistoryTable(std::shared_ptr<Table> table);

  Result<std::unique_ptr<MetadataTableScan>> MakeScan(
      std::shared_ptr<TableMetadata> metadata, std::shared_ptr<FileIO> io,
      internal::TableScanContext context) const override;
};

}  // namespace iceberg
