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

#include "iceberg/inspect/static_data_task_internal.h"

#include <cstddef>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "iceberg/row/struct_like.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg {
namespace {

class ProjectedStructLike : public StructLike {
 public:
  ProjectedStructLike(std::shared_ptr<StructLike> row,
                      std::shared_ptr<const std::vector<size_t>> positions)
      : row_(std::move(row)), positions_(std::move(positions)) {}

  Result<Scalar> GetField(size_t pos) const override {
    if (pos >= positions_->size()) {
      return InvalidArgument("Invalid projected field index: {}", pos);
    }
    return row_->GetField((*positions_)[pos]);
  }

  size_t num_fields() const override { return positions_->size(); }

 private:
  std::shared_ptr<StructLike> row_;
  std::shared_ptr<const std::vector<size_t>> positions_;
};

class StaticRowStream : public StructLikeStream {
 public:
  StaticRowStream(const std::vector<std::shared_ptr<StructLike>>& rows,
                  std::shared_ptr<const std::vector<size_t>> positions)
      : rows_(rows), positions_(std::move(positions)) {}

 protected:
  Result<std::optional<std::shared_ptr<StructLike>>> NextImpl() override {
    if (next_row_ == rows_.size()) {
      return std::nullopt;
    }
    const auto& row = rows_[next_row_++];
    if (positions_ == nullptr) {
      return row;
    }
    return std::make_shared<ProjectedStructLike>(row, positions_);
  }

 private:
  const std::vector<std::shared_ptr<StructLike>>& rows_;
  std::shared_ptr<const std::vector<size_t>> positions_;
  size_t next_row_ = 0;
};

}  // namespace

StaticDataTask::StaticDataTask(std::shared_ptr<Schema> projected_schema,
                               std::shared_ptr<const std::vector<size_t>> positions,
                               std::vector<std::shared_ptr<StructLike>> rows)
    : projected_schema_(std::move(projected_schema)),
      positions_(std::move(positions)),
      rows_(std::move(rows)) {}

StaticDataTask::~StaticDataTask() = default;

Result<std::shared_ptr<StaticDataTask>> StaticDataTask::Make(
    const Schema& table_schema, std::shared_ptr<Schema> projected_schema,
    std::vector<std::shared_ptr<StructLike>> rows) {
  ICEBERG_PRECHECK(projected_schema != nullptr, "Projected schema cannot be null");

  const auto table_fields = table_schema.fields();
  std::vector<size_t> positions;
  positions.reserve(projected_schema->fields().size());
  for (const auto& projected_field : projected_schema->fields()) {
    std::optional<size_t> position;
    for (size_t i = 0; i < table_fields.size(); ++i) {
      if (table_fields[i].field_id() == projected_field.field_id()) {
        position = i;
        break;
      }
    }
    ICEBERG_PRECHECK(position.has_value(), "Cannot find projected field {} (id {})",
                     projected_field.name(), projected_field.field_id());
    if (*table_fields[*position].type() != *projected_field.type()) {
      return NotSupported("Cannot project only part of nested field {}",
                          projected_field.name());
    }
    positions.push_back(*position);
  }

  bool is_identity = positions.size() == table_fields.size();
  for (size_t i = 0; is_identity && i < positions.size(); ++i) {
    is_identity = positions[i] == i;
  }

  std::shared_ptr<const std::vector<size_t>> shared_positions;
  if (!is_identity) {
    shared_positions = std::make_shared<const std::vector<size_t>>(std::move(positions));
  }
  return std::shared_ptr<StaticDataTask>(new StaticDataTask(
      std::move(projected_schema), std::move(shared_positions), std::move(rows)));
}

Result<std::unique_ptr<StructLikeStream>> StaticDataTask::Rows() const {
  return std::make_unique<StaticRowStream>(rows_, positions_);
}

}  // namespace iceberg
