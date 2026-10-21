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

/// \file iceberg/data_task_reader.h
/// \brief Read DataTask rows as Arrow C Data batches.

#include <cstdint>
#include <memory>
#include <vector>

#include "iceberg/arrow_c_data.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Default maximum number of rows in each batch read from data tasks.
inline constexpr int64_t kDefaultDataTaskBatchSize = 1024;

/// \brief Read the rows of data tasks as one Arrow stream.
///
/// Tasks are read in order and a batch may hold rows from more than one task, so a
/// caller that wants to read tasks in parallel passes one task per call.
///
/// \param schema Schema of the returned stream. Every task must return rows in it.
/// \param tasks Tasks to read. The stream keeps them alive until it is released.
/// \param batch_size Maximum number of rows in each batch. Must be positive.
/// \return A stream that the caller owns and must release by invoking its `release`
/// callback, or an error.
ICEBERG_EXPORT Result<ArrowArrayStream> ReadDataTasks(
    std::shared_ptr<Schema> schema, std::vector<std::shared_ptr<DataTask>> tasks,
    int64_t batch_size = kDefaultDataTaskBatchSize);

}  // namespace iceberg
