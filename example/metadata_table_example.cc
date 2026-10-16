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

// Reads the snapshots and history metadata tables of an existing Iceberg table: the
// snapshots table as Arrow batches, and the history table as rows without Arrow.

#include <cstdint>
#include <iostream>
#include <string>
#include <variant>

#include "iceberg/arrow/arrow_io_util.h"
#include "iceberg/catalog/memory/in_memory_catalog.h"
#include "iceberg/inspect/history_table.h"
#include "iceberg/inspect/metadata_table.h"
#include "iceberg/inspect/metadata_table_reader.h"
#include "iceberg/inspect/snapshots_table.h"
#include "iceberg/result.h"
#include "iceberg/row/struct_like.h"
#include "iceberg/table.h"
#include "iceberg/table_scan.h"
#include "iceberg/util/macros.h"

namespace {

// ICEBERG_ASSIGN_OR_RAISE names Error without its namespace.
using iceberg::Error;

// Read the snapshots table as an ArrowArrayStream. Any Arrow consumer can take the
// stream; this one counts rows through the Arrow C stream interface.
iceberg::Status PrintSnapshotCount(const iceberg::SnapshotsTable& snapshots) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto scan, snapshots.NewScan()->Select({"snapshot_id", "operation"}).Build());
  ICEBERG_ASSIGN_OR_RAISE(ArrowArrayStream stream, iceberg::ReadMetadataTable(*scan));

  int64_t num_rows = 0;
  while (true) {
    ArrowArray batch{};
    if (stream.get_next(&stream, &batch) != 0) {
      const char* message = stream.get_last_error(&stream);
      std::string error = message != nullptr ? message : "unknown error";
      stream.release(&stream);
      return iceberg::IOError("Failed to read the snapshots table: {}", error);
    }
    if (batch.release == nullptr) {
      break;
    }
    num_rows += batch.length;
    batch.release(&batch);
  }
  stream.release(&stream);

  std::cout << "Snapshots: " << num_rows << std::endl;
  return {};
}

// Read the history table row by row, without converting to Arrow.
iceberg::Status PrintHistory(const iceberg::HistoryTable& history) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto scan,
      history.NewScan()->Select({"snapshot_id", "is_current_ancestor"}).Build());
  ICEBERG_ASSIGN_OR_RAISE(auto tasks, scan->PlanFiles());

  std::cout << "History:" << std::endl;
  for (const auto& task : tasks) {
    ICEBERG_ASSIGN_OR_RAISE(auto rows, task->Rows());
    while (true) {
      ICEBERG_ASSIGN_OR_RAISE(auto row, rows->Next());
      if (!row.has_value()) {
        break;
      }
      // Fields follow the scan's schema, which keeps the table's column order.
      ICEBERG_ASSIGN_OR_RAISE(auto snapshot_id, (*row)->GetField(0));
      ICEBERG_ASSIGN_OR_RAISE(auto is_current_ancestor, (*row)->GetField(1));
      std::cout << " - " << std::get<int64_t>(snapshot_id)
                << (std::get<bool>(is_current_ancestor) ? ""
                                                        : " (not a current ancestor)")
                << std::endl;
    }
  }
  return {};
}

iceberg::Status Run(const std::string& warehouse_location, const std::string& table_name,
                    const std::string& metadata_location) {
  ICEBERG_ASSIGN_OR_RAISE(auto catalog, iceberg::InMemoryCatalog::Make(
                                            "example", iceberg::arrow::MakeLocalFileIO(),
                                            warehouse_location, /*properties=*/{}));
  ICEBERG_ASSIGN_OR_RAISE(
      auto table, catalog->RegisterTable({.name = table_name}, metadata_location));

  ICEBERG_ASSIGN_OR_RAISE(auto snapshots,
                          iceberg::MetadataTable::Make<iceberg::SnapshotsTable>(table));
  ICEBERG_RETURN_UNEXPECTED(PrintSnapshotCount(*snapshots));

  ICEBERG_ASSIGN_OR_RAISE(auto history,
                          iceberg::MetadataTable::Make<iceberg::HistoryTable>(table));
  return PrintHistory(*history);
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 4) {
    std::cerr << "Usage: " << argv[0]
              << " <warehouse_location> <table_name> <metadata_location>" << std::endl;
    return 0;
  }

  auto status = Run(argv[1], argv[2], argv[3]);
  if (!status.has_value()) {
    std::cerr << status.error().message << std::endl;
    return 1;
  }
  return 0;
}
