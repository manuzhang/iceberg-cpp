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

#include "iceberg/inspect/metadata_table.h"

#include <memory>
#include <utility>

#include "iceberg/table.h"

namespace iceberg {

MetadataTable::MetadataTable(std::shared_ptr<Table> source_table)
    : source_table_(std::move(source_table)) {}

MetadataTable::~MetadataTable() = default;

bool MetadataTable::supports_time_travel() const noexcept { return false; }

const std::shared_ptr<Table>& MetadataTable::source_table() const {
  return source_table_;
}

std::unique_ptr<MetadataTableScanBuilder> MetadataTable::NewScan() const {
  return std::unique_ptr<MetadataTableScanBuilder>(new MetadataTableScanBuilder(
      *this, source_table_->metadata(), source_table_->io()));
}

}  // namespace iceberg
