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

#include "iceberg/data_task_reader.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow_c_data_util_internal.h"
#include "iceberg/arrow_row_builder_internal.h"
#include "iceberg/nanoarrow_status_internal.h"
#include "iceberg/row/struct_like.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/schema_internal.h"
#include "iceberg/table_scan.h"
#include "iceberg/type.h"
#include "iceberg/util/decimal.h"
#include "iceberg/util/macros.h"

namespace iceberg {
namespace {

Status AppendScalar(ArrowArray* array, const SchemaField& field, const Scalar& value);

Status AppendNullField(ArrowArray* array, const SchemaField& field) {
  ICEBERG_PRECHECK(field.optional(), "Required data task field {} is null", field.name());
  return AppendNull(array);
}

template <typename T>
bool ScalarMatchesType(TypeId type_id) {
  if constexpr (std::is_same_v<T, bool>) {
    return type_id == TypeId::kBoolean;
  } else if constexpr (std::is_same_v<T, int32_t>) {
    return type_id == TypeId::kInt || type_id == TypeId::kDate;
  } else if constexpr (std::is_same_v<T, int64_t>) {
    return type_id == TypeId::kLong || type_id == TypeId::kTime ||
           type_id == TypeId::kTimestamp || type_id == TypeId::kTimestampTz ||
           type_id == TypeId::kTimestampNs || type_id == TypeId::kTimestampTzNs;
  } else if constexpr (std::is_same_v<T, float>) {
    return type_id == TypeId::kFloat;
  } else if constexpr (std::is_same_v<T, double>) {
    return type_id == TypeId::kDouble;
  } else if constexpr (std::is_same_v<T, std::string_view>) {
    return type_id == TypeId::kString || type_id == TypeId::kBinary ||
           type_id == TypeId::kFixed || type_id == TypeId::kUuid ||
           type_id == TypeId::kGeometry || type_id == TypeId::kGeography;
  } else if constexpr (std::is_same_v<T, Decimal>) {
    return type_id == TypeId::kDecimal;
  } else if constexpr (std::is_same_v<T, std::shared_ptr<StructLike>>) {
    return type_id == TypeId::kStruct;
  } else if constexpr (std::is_same_v<T, std::shared_ptr<ArrayLike>>) {
    return type_id == TypeId::kList;
  } else {
    static_assert(std::is_same_v<T, std::shared_ptr<MapLike>>);
    return type_id == TypeId::kMap;
  }
}

Status AppendStruct(ArrowArray* array, const StructType& type, const StructLike& value) {
  if (std::cmp_not_equal(value.num_fields(), array->n_children)) {
    return InvalidArgument("Cannot append a struct with {} fields to {} Arrow children",
                           value.num_fields(), array->n_children);
  }
  for (int64_t i = 0; i < array->n_children; ++i) {
    ICEBERG_ASSIGN_OR_RAISE(auto field, value.GetField(static_cast<size_t>(i)));
    ICEBERG_RETURN_UNEXPECTED(AppendScalar(array->children[i], type.fields()[i], field));
  }
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(array));
  return {};
}

Status AppendList(ArrowArray* array, const ListType& type, const ArrayLike& value) {
  if (array->n_children != 1) {
    return InvalidArrowData("List array must have exactly 1 child");
  }
  for (size_t i = 0; i < value.size(); ++i) {
    ICEBERG_ASSIGN_OR_RAISE(auto element, value.GetElement(i));
    ICEBERG_RETURN_UNEXPECTED(AppendScalar(array->children[0], type.element(), element));
  }
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(array));
  return {};
}

Status AppendMap(ArrowArray* array, const MapType& type, const MapLike& value) {
  // A map array is a list of struct<key, value>.
  if (array->n_children != 1 || array->children[0]->n_children != 2) {
    return InvalidArrowData("Map array must have one entries child with 2 children");
  }
  ArrowArray* entries = array->children[0];
  for (size_t i = 0; i < value.size(); ++i) {
    ICEBERG_ASSIGN_OR_RAISE(auto key, value.GetKey(i));
    ICEBERG_RETURN_UNEXPECTED(AppendScalar(entries->children[0], type.key(), key));
    ICEBERG_ASSIGN_OR_RAISE(auto item, value.GetValue(i));
    ICEBERG_RETURN_UNEXPECTED(AppendScalar(entries->children[1], type.value(), item));
    ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(entries));
  }
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(array));
  return {};
}

// Validate the row's scalar alternative before letting nanoarrow encode it.
Status AppendScalar(ArrowArray* array, const SchemaField& field, const Scalar& value) {
  return std::visit(
      [array, &field]<typename T>(const T& v) -> Status {
        if constexpr (std::is_same_v<T, std::monostate>) {
          return AppendNullField(array, field);
        } else {
          if constexpr (std::is_same_v<T, std::shared_ptr<StructLike>> ||
                        std::is_same_v<T, std::shared_ptr<ArrayLike>> ||
                        std::is_same_v<T, std::shared_ptr<MapLike>>) {
            if (v == nullptr) {
              return AppendNullField(array, field);
            }
          }
          ICEBERG_PRECHECK(ScalarMatchesType<T>(field.type()->type_id()),
                           "Data task field {} has a scalar that does not match type {}",
                           field.name(), field.type()->ToString());
          if constexpr (std::is_same_v<T, bool>) {
            return AppendBoolean(array, v);
          } else if constexpr (std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>) {
            return AppendInt(array, v);
          } else if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double>) {
            return AppendDouble(array, v);
          } else if constexpr (std::is_same_v<T, std::string_view>) {
            return AppendBytes(array,
                               std::span<const uint8_t>(
                                   reinterpret_cast<const uint8_t*>(v.data()),  // NOLINT
                                   v.size()));
          } else if constexpr (std::is_same_v<T, Decimal>) {
            return AppendDecimal(array, v);
          } else if constexpr (std::is_same_v<T, std::shared_ptr<StructLike>>) {
            return AppendStruct(array, static_cast<const StructType&>(*field.type()), *v);
          } else if constexpr (std::is_same_v<T, std::shared_ptr<ArrayLike>>) {
            return AppendList(array, static_cast<const ListType&>(*field.type()), *v);
          } else {
            static_assert(std::is_same_v<T, std::shared_ptr<MapLike>>);
            return AppendMap(array, static_cast<const MapType&>(*field.type()), *v);
          }
        }
      },
      value);
}

Status AppendRow(ArrowRowBuilder& builder, const Schema& schema, const StructLike& row) {
  if (std::cmp_not_equal(row.num_fields(), builder.num_columns())) {
    return InvalidArgument("Data task row has {} fields but the schema has {} columns",
                           row.num_fields(), builder.num_columns());
  }
  for (int64_t i = 0; i < builder.num_columns(); ++i) {
    ICEBERG_ASSIGN_OR_RAISE(auto field, row.GetField(static_cast<size_t>(i)));
    ICEBERG_RETURN_UNEXPECTED(AppendScalar(builder.column(i), schema.fields()[i], field));
  }
  return builder.FinishRow();
}

class DataTaskArrowSource {
 public:
  static Result<std::unique_ptr<DataTaskArrowSource>> Make(
      std::shared_ptr<iceberg::Schema> schema,
      std::vector<std::shared_ptr<DataTask>> tasks, int64_t batch_size) {
    ArrowSchema arrow_schema{};
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(*schema, &arrow_schema));
    return std::unique_ptr<DataTaskArrowSource>(new DataTaskArrowSource(
        std::move(schema), std::move(tasks), arrow_schema, batch_size));
  }

  ~DataTaskArrowSource() { std::ignore = Close(); }

  Status Close() {
    rows_.reset();
    tasks_.clear();
    if (arrow_schema_.release != nullptr) {
      ArrowSchemaRelease(&arrow_schema_);
    }
    return {};
  }

  Result<std::optional<ArrowArray>> Next() {
    if (arrow_schema_.release == nullptr) [[unlikely]] {
      return InvalidArgument("Cannot read from a closed data task stream");
    }

    // Fetch a row before allocating a builder, so an exhausted stream allocates none.
    ICEBERG_ASSIGN_OR_RAISE(auto row, NextRow());
    if (row == nullptr) {
      return std::nullopt;
    }

    ICEBERG_ASSIGN_OR_RAISE(auto builder, ArrowRowBuilder::Make(&arrow_schema_));
    while (true) {
      ICEBERG_RETURN_UNEXPECTED(AppendRow(builder, *schema_, *row));
      if (builder.num_rows() == batch_size_) {
        break;
      }
      ICEBERG_ASSIGN_OR_RAISE(row, NextRow());
      if (row == nullptr) {
        break;
      }
    }

    ICEBERG_ASSIGN_OR_RAISE(auto array, std::move(builder).Finish());
    return array;
  }

  Result<ArrowSchema> Schema() {
    if (arrow_schema_.release == nullptr) [[unlikely]] {
      return InvalidArgument("Cannot read schema from a closed data task stream");
    }
    ArrowSchema schema_copy{};
    ICEBERG_NANOARROW_RETURN_UNEXPECTED(
        ArrowSchemaDeepCopy(&arrow_schema_, &schema_copy));
    return schema_copy;
  }

 private:
  DataTaskArrowSource(std::shared_ptr<iceberg::Schema> schema,
                      std::vector<std::shared_ptr<DataTask>> tasks,
                      ArrowSchema arrow_schema, int64_t batch_size)
      : schema_(std::move(schema)),
        tasks_(std::move(tasks)),
        arrow_schema_(arrow_schema),
        batch_size_(batch_size) {}

  // Return the next row across all tasks, or nullptr once every task is exhausted.
  Result<std::shared_ptr<StructLike>> NextRow() {
    while (true) {
      if (rows_ == nullptr) {
        if (next_task_ == tasks_.size()) {
          return nullptr;
        }
        ICEBERG_ASSIGN_OR_RAISE(rows_, tasks_[next_task_++]->Rows());
        ICEBERG_PRECHECK(rows_ != nullptr, "Data task returned a null row stream");
      }
      ICEBERG_ASSIGN_OR_RAISE(auto row, rows_->Next());
      if (!row.has_value()) {
        rows_.reset();
        continue;
      }
      if (*row == nullptr) [[unlikely]] {
        return InvalidArgument("Data task returned a null row");
      }
      return std::move(*row);
    }
  }

  std::shared_ptr<iceberg::Schema> schema_;
  std::vector<std::shared_ptr<DataTask>> tasks_;
  size_t next_task_ = 0;
  std::unique_ptr<StructLikeStream> rows_;
  ArrowSchema arrow_schema_{};
  int64_t batch_size_;
};

}  // namespace

Result<ArrowArrayStream> ReadDataTasks(std::shared_ptr<Schema> schema,
                                       std::vector<std::shared_ptr<DataTask>> tasks,
                                       int64_t batch_size) {
  ICEBERG_PRECHECK(schema != nullptr, "Schema cannot be null");
  ICEBERG_PRECHECK(batch_size > 0, "Batch size must be positive, got {}", batch_size);
  for (const auto& task : tasks) {
    ICEBERG_PRECHECK(task != nullptr, "Data task cannot be null");
    ICEBERG_PRECHECK(task->schema() != nullptr, "Data task schema cannot be null");
    ICEBERG_PRECHECK(task->schema() == schema || *task->schema() == *schema,
                     "Data task schema does not match the requested schema");
  }

  ICEBERG_ASSIGN_OR_RAISE(
      auto source,
      DataTaskArrowSource::Make(std::move(schema), std::move(tasks), batch_size));
  return MakeArrowArrayStream(std::move(source));
}

}  // namespace iceberg
