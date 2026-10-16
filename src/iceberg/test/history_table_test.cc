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

/// \file history_table_test.cc
/// Unit tests for HistoryTable.

#include "iceberg/inspect/history_table.h"

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/inspect/metadata_table.h"
#include "iceberg/inspect/metadata_table_reader.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/metadata_table_test_base.h"
#include "iceberg/type.h"
#include "iceberg/util/timepoint.h"

namespace iceberg {
namespace {

std::shared_ptr<Schema> MakeHistorySchema() {
  return std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "made_current_at", timestamp_tz()),
      SchemaField::MakeRequired(2, "snapshot_id", int64()),
      SchemaField::MakeOptional(3, "parent_id", int64()),
      SchemaField::MakeRequired(4, "is_current_ancestor", boolean())});
}

}  // namespace

class HistoryTableTest : public MetadataTableTestBase {
 protected:
  Result<std::shared_ptr<Table>> MakeTableWithHistory(
      std::vector<std::shared_ptr<Snapshot>> snapshots, int64_t current_snapshot_id,
      std::vector<SnapshotLogEntry> snapshot_log) {
    auto metadata = std::make_shared<TableMetadata>(*metadata_);
    metadata->snapshots = std::move(snapshots);
    metadata->current_snapshot_id = current_snapshot_id;
    metadata->snapshot_log = std::move(snapshot_log);
    TableIdentifier ident{.ns = Namespace{.levels = {"db"}}, .name = "history_table"};
    return Table::Make(ident, metadata, "s3://bucket/meta.json", io_, catalog_);
  }

  static SnapshotLogEntry LogEntry(int64_t timestamp_ms, int64_t snapshot_id) {
    return {.timestamp_ms = TimePointMsFromUnixMs(timestamp_ms),
            .snapshot_id = snapshot_id};
  }
};

TEST_F(HistoryTableTest, SchemaMatchesIcebergSchema) {
  ICEBERG_UNWRAP_OR_FAIL(auto history_table, MetadataTable::Make<HistoryTable>(table_));
  EXPECT_TRUE(*history_table->schema() == *MakeHistorySchema());
}

TEST_F(HistoryTableTest, ScanAfterRollback) {
  auto [snap1, snap2] = MakeTestSnapshots();
  auto snap3 = std::make_shared<Snapshot>(*snap2);
  snap3->snapshot_id = 3;
  ICEBERG_UNWRAP_OR_FAIL(
      auto table, MakeTableWithHistory({snap1, snap2, snap3}, /*current_snapshot_id=*/3,
                                       {LogEntry(1000, 1), LogEntry(2000, 2),
                                        LogEntry(3000, 1), LogEntry(4000, 3)}));
  ICEBERG_UNWRAP_OR_FAIL(auto history_table, MetadataTable::Make<HistoryTable>(table));

  ICEBERG_UNWRAP_OR_FAIL(auto stream, ScanToArrowStream(*history_table));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  const auto& batch = batches.front();
  ASSERT_EQ(batch->num_rows(), 4);
  ASSERT_EQ(batch->num_columns(), 4);

  auto made_current_at =
      std::static_pointer_cast<::arrow::TimestampArray>(batch->column(0));
  EXPECT_EQ(made_current_at->Value(0), 1000 * 1000);
  EXPECT_EQ(made_current_at->Value(1), 2000 * 1000);
  EXPECT_EQ(made_current_at->Value(2), 3000 * 1000);
  EXPECT_EQ(made_current_at->Value(3), 4000 * 1000);

  auto snapshot_ids = std::static_pointer_cast<::arrow::Int64Array>(batch->column(1));
  EXPECT_EQ(snapshot_ids->Value(0), 1);
  EXPECT_EQ(snapshot_ids->Value(1), 2);
  EXPECT_EQ(snapshot_ids->Value(2), 1);
  EXPECT_EQ(snapshot_ids->Value(3), 3);

  auto parent_ids = std::static_pointer_cast<::arrow::Int64Array>(batch->column(2));
  EXPECT_TRUE(parent_ids->IsNull(0));
  EXPECT_EQ(parent_ids->Value(1), 1);
  EXPECT_TRUE(parent_ids->IsNull(2));
  EXPECT_EQ(parent_ids->Value(3), 1);

  auto is_current_ancestor =
      std::static_pointer_cast<::arrow::BooleanArray>(batch->column(3));
  EXPECT_TRUE(is_current_ancestor->Value(0));
  EXPECT_FALSE(is_current_ancestor->Value(1));
  EXPECT_TRUE(is_current_ancestor->Value(2));
  EXPECT_TRUE(is_current_ancestor->Value(3));
}

TEST_F(HistoryTableTest, ScanEntryForMissingSnapshot) {
  auto snap1 = MakeTestSnapshots().first;
  ICEBERG_UNWRAP_OR_FAIL(auto table,
                         MakeTableWithHistory({snap1}, /*current_snapshot_id=*/1,
                                              {LogEntry(500, 99), LogEntry(1000, 1)}));
  ICEBERG_UNWRAP_OR_FAIL(auto history_table, MetadataTable::Make<HistoryTable>(table));

  ICEBERG_UNWRAP_OR_FAIL(auto stream, ScanToArrowStream(*history_table));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  const auto& batch = batches.front();
  ASSERT_EQ(batch->num_rows(), 2);

  auto snapshot_ids = std::static_pointer_cast<::arrow::Int64Array>(batch->column(1));
  EXPECT_EQ(snapshot_ids->Value(0), 99);
  auto parent_ids = std::static_pointer_cast<::arrow::Int64Array>(batch->column(2));
  EXPECT_TRUE(parent_ids->IsNull(0));
  auto is_current_ancestor =
      std::static_pointer_cast<::arrow::BooleanArray>(batch->column(3));
  EXPECT_FALSE(is_current_ancestor->Value(0));
  EXPECT_TRUE(is_current_ancestor->Value(1));
}

TEST_F(HistoryTableTest, ScanTableWithoutSnapshots) {
  ICEBERG_UNWRAP_OR_FAIL(auto history_table, MetadataTable::Make<HistoryTable>(table_));

  ICEBERG_UNWRAP_OR_FAIL(auto stream, ScanToArrowStream(*history_table));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  EXPECT_TRUE(batches.empty());
}

TEST_F(HistoryTableTest, ScanSelectedColumns) {
  auto snap1 = MakeTestSnapshots().first;
  ICEBERG_UNWRAP_OR_FAIL(
      auto table,
      MakeTableWithHistory({snap1}, /*current_snapshot_id=*/1, {LogEntry(1000, 1)}));
  ICEBERG_UNWRAP_OR_FAIL(auto history_table, MetadataTable::Make<HistoryTable>(table));

  ICEBERG_UNWRAP_OR_FAIL(
      auto scan,
      history_table->NewScan()->Select({"is_current_ancestor", "snapshot_id"}).Build());
  ICEBERG_UNWRAP_OR_FAIL(auto stream, ReadMetadataTable(*scan));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  const auto& batch = batches.front();
  ASSERT_EQ(batch->num_columns(), 2);
  EXPECT_EQ(batch->schema()->field(0)->name(), "snapshot_id");
  EXPECT_EQ(batch->schema()->field(1)->name(), "is_current_ancestor");
  EXPECT_EQ(std::static_pointer_cast<::arrow::Int64Array>(batch->column(0))->Value(0), 1);
  EXPECT_TRUE(
      std::static_pointer_cast<::arrow::BooleanArray>(batch->column(1))->Value(0));
}

}  // namespace iceberg
