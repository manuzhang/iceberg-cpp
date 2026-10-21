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

#include "iceberg/inspect/metadata_table_reader.h"

#include <utility>

#include "iceberg/inspect/metadata_table_scan.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Result<ArrowArrayStream> ReadMetadataTable(const MetadataTableScan& scan,
                                           int64_t batch_size) {
  ICEBERG_ASSIGN_OR_RAISE(auto schema, scan.schema());
  ICEBERG_ASSIGN_OR_RAISE(auto tasks, scan.PlanFiles());
  return ReadDataTasks(std::move(schema), std::move(tasks), batch_size);
}

}  // namespace iceberg
