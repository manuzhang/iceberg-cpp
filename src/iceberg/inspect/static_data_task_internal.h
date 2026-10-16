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

/// \file iceberg/inspect/static_data_task_internal.h
/// \brief A data task over metadata table rows that are already in memory.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/table_scan.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief A data task over rows that are already in memory.
///
/// A metadata table builds every row in its full schema, and Rows() returns them
/// projected to the scan's schema, so each table needs no projection logic of its own.
class ICEBERG_EXPORT StaticDataTask : public DataTask {
 public:
  /// \brief Create a task over in-memory rows.
  ///
  /// \param table_schema Schema of `rows`.
  /// \param projected_schema Schema of the rows returned by Rows(). It may select and
  /// reorder complete top-level fields of `table_schema`; selecting only part of a
  /// nested struct is not supported.
  /// \param rows Rows whose fields follow `table_schema` by position.
  static Result<std::shared_ptr<StaticDataTask>> Make(
      const Schema& table_schema, std::shared_ptr<Schema> projected_schema,
      std::vector<std::shared_ptr<StructLike>> rows);

  ~StaticDataTask() override;

  const std::shared_ptr<Schema>& schema() const override { return projected_schema_; }

  Result<std::unique_ptr<StructLikeStream>> Rows() const override;

  /// \brief Return zero because the rows are already in memory.
  int64_t size_bytes() const override { return 0; }

  /// \brief Return zero because reading the rows opens no file.
  int32_t files_count() const override { return 0; }

  int64_t estimated_row_count() const override {
    return static_cast<int64_t>(rows_.size());
  }

 private:
  StaticDataTask(std::shared_ptr<Schema> projected_schema,
                 std::shared_ptr<const std::vector<size_t>> positions,
                 std::vector<std::shared_ptr<StructLike>> rows);

  std::shared_ptr<Schema> projected_schema_;
  // Position in the table schema of each projected field; null when the projection
  // keeps every field in order.
  std::shared_ptr<const std::vector<size_t>> positions_;
  std::vector<std::shared_ptr<StructLike>> rows_;
};

}  // namespace iceberg
