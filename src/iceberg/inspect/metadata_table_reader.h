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

/// \file iceberg/inspect/metadata_table_reader.h
/// \brief Read metadata table scans as Arrow C Data batches.

#include <cstdint>

#include "iceberg/arrow_c_data.h"
#include "iceberg/data_task_reader.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Plan a metadata table scan and read all of its rows as one Arrow stream.
///
/// The stream has the scan's schema and reads the tasks planned by
/// MetadataTableScan::PlanFiles() in order. To read tasks in parallel, plan them with
/// PlanFiles() and pass one task to each ReadDataTasks() call instead.
///
/// \param scan The scan to read.
/// \param batch_size Maximum number of rows in each batch. Must be positive.
/// \return A stream that the caller owns and must release by invoking its `release`
/// callback, or an error.
ICEBERG_EXPORT Result<ArrowArrayStream> ReadMetadataTable(
    const MetadataTableScan& scan, int64_t batch_size = kDefaultDataTaskBatchSize);

}  // namespace iceberg
