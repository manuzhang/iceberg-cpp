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

#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/data_task_reader.h"
#include "iceberg/inspect/static_data_task_internal.h"
#include "iceberg/row/struct_like.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/table_scan.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"
#include "iceberg/util/decimal.h"

namespace iceberg {
namespace {

class TestStruct : public StructLike {
 public:
  explicit TestStruct(std::vector<Scalar> fields) : fields_(std::move(fields)) {}

  Result<Scalar> GetField(size_t pos) const override {
    if (pos >= fields_.size()) {
      return InvalidArgument("Invalid field index: {}", pos);
    }
    return fields_[pos];
  }

  size_t num_fields() const override { return fields_.size(); }

 private:
  std::vector<Scalar> fields_;
};

class TestList : public ArrayLike {
 public:
  explicit TestList(std::vector<Scalar> elements) : elements_(std::move(elements)) {}

  Result<Scalar> GetElement(size_t pos) const override { return elements_.at(pos); }

  size_t size() const override { return elements_.size(); }

 private:
  std::vector<Scalar> elements_;
};

class TestMap : public MapLike {
 public:
  explicit TestMap(std::vector<std::pair<Scalar, Scalar>> entries)
      : entries_(std::move(entries)) {}

  Result<Scalar> GetKey(size_t pos) const override { return entries_.at(pos).first; }

  Result<Scalar> GetValue(size_t pos) const override { return entries_.at(pos).second; }

  size_t size() const override { return entries_.size(); }

 private:
  std::vector<std::pair<Scalar, Scalar>> entries_;
};

std::shared_ptr<StructLike> MakeRow(std::vector<Scalar> fields) {
  return std::make_shared<TestStruct>(std::move(fields));
}

std::shared_ptr<Schema> MakeIdSchema() {
  return std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32())});
}

Result<std::vector<std::shared_ptr<::arrow::RecordBatch>>> ReadAllBatches(
    ArrowArrayStream&& stream) {
  auto reader = ::arrow::ImportRecordBatchReader(&stream);
  if (!reader.ok()) {
    if (stream.release != nullptr) {
      stream.release(&stream);
    }
    return InvalidArrowData(reader.status().ToString());
  }
  auto batches = (*reader)->ToRecordBatches();
  if (!batches.ok()) {
    return InvalidArrowData(batches.status().ToString());
  }
  return std::move(batches).MoveValueUnsafe();
}

Result<std::vector<std::shared_ptr<::arrow::RecordBatch>>> ReadSingleField(
    SchemaField field, Scalar value) {
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{std::move(field)});
  ICEBERG_ASSIGN_OR_RAISE(
      auto task, StaticDataTask::Make(*schema, schema, {MakeRow({std::move(value)})}));
  ICEBERG_ASSIGN_OR_RAISE(auto stream, ReadDataTasks(schema, {task}));
  return ReadAllBatches(std::move(stream));
}

class NullRowStreamTask : public DataTask {
 public:
  explicit NullRowStreamTask(std::shared_ptr<Schema> schema)
      : schema_(std::move(schema)) {}

  const std::shared_ptr<Schema>& schema() const override { return schema_; }
  Result<std::unique_ptr<StructLikeStream>> Rows() const override { return nullptr; }
  int64_t size_bytes() const override { return 0; }
  int32_t files_count() const override { return 0; }
  int64_t estimated_row_count() const override { return 0; }

 private:
  std::shared_ptr<Schema> schema_;
};

}  // namespace

TEST(DataTaskReaderTest, ReadsNestedValues) {
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeOptional(2, "name", string()),
      SchemaField::MakeOptional(3, "point",
                                std::make_shared<StructType>(std::vector<SchemaField>{
                                    SchemaField::MakeRequired(4, "x", float64()),
                                    SchemaField::MakeOptional(5, "label", binary())})),
      SchemaField::MakeOptional(
          6, "tags",
          std::make_shared<ListType>(SchemaField::MakeRequired(7, "element", int64()))),
      SchemaField::MakeOptional(
          8, "props",
          std::make_shared<MapType>(SchemaField::MakeRequired(9, "key", string()),
                                    SchemaField::MakeOptional(10, "value", int32()))),
      SchemaField::MakeOptional(11, "hash", fixed(4)),
      SchemaField::MakeOptional(12, "price", decimal(9, 2)),
  });

  std::vector<std::shared_ptr<StructLike>> rows{
      MakeRow({int32_t{1}, std::string_view("a"), MakeRow({1.5, std::string_view("xy")}),
               std::make_shared<TestList>(std::vector<Scalar>{int64_t{10}, int64_t{20}}),
               std::make_shared<TestMap>(std::vector<std::pair<Scalar, Scalar>>{
                   {std::string_view("k1"), int32_t{1}},
                   {std::string_view("k2"), std::monostate{}}}),
               std::string_view("abcd"), Decimal(12345)}),
      MakeRow({int32_t{2}, std::monostate{}, std::monostate{}, std::monostate{},
               std::make_shared<TestMap>(std::vector<std::pair<Scalar, Scalar>>{}),
               std::monostate{}, std::monostate{}}),
  };
  ICEBERG_UNWRAP_OR_FAIL(auto task, StaticDataTask::Make(*schema, schema, rows));

  ICEBERG_UNWRAP_OR_FAIL(auto stream, ReadDataTasks(schema, {task}));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  const auto& batch = batches.front();
  ASSERT_EQ(batch->num_rows(), 2);

  auto ids = std::static_pointer_cast<::arrow::Int32Array>(batch->column(0));
  EXPECT_EQ(ids->Value(0), 1);
  EXPECT_EQ(ids->Value(1), 2);

  auto names = std::static_pointer_cast<::arrow::StringArray>(batch->column(1));
  EXPECT_EQ(names->GetString(0), "a");
  EXPECT_TRUE(names->IsNull(1));

  auto points = std::static_pointer_cast<::arrow::StructArray>(batch->column(2));
  EXPECT_FALSE(points->IsNull(0));
  EXPECT_TRUE(points->IsNull(1));
  EXPECT_EQ(std::static_pointer_cast<::arrow::DoubleArray>(points->field(0))->Value(0),
            1.5);
  EXPECT_EQ(
      std::static_pointer_cast<::arrow::BinaryArray>(points->field(1))->GetString(0),
      "xy");

  auto tags = std::static_pointer_cast<::arrow::ListArray>(batch->column(3));
  EXPECT_TRUE(tags->IsNull(1));
  ASSERT_EQ(tags->value_length(0), 2);
  auto tag_values = std::static_pointer_cast<::arrow::Int64Array>(tags->values());
  EXPECT_EQ(tag_values->Value(0), 10);
  EXPECT_EQ(tag_values->Value(1), 20);

  auto props = std::static_pointer_cast<::arrow::MapArray>(batch->column(4));
  ASSERT_EQ(props->value_length(0), 2);
  EXPECT_FALSE(props->IsNull(1));
  EXPECT_EQ(props->value_length(1), 0);
  auto prop_keys = std::static_pointer_cast<::arrow::StringArray>(props->keys());
  auto prop_values = std::static_pointer_cast<::arrow::Int32Array>(props->items());
  EXPECT_EQ(prop_keys->GetString(0), "k1");
  EXPECT_EQ(prop_values->Value(0), 1);
  EXPECT_EQ(prop_keys->GetString(1), "k2");
  EXPECT_TRUE(prop_values->IsNull(1));

  auto hashes = std::static_pointer_cast<::arrow::FixedSizeBinaryArray>(batch->column(5));
  EXPECT_EQ(hashes->GetString(0), "abcd");
  EXPECT_TRUE(hashes->IsNull(1));

  auto prices = std::static_pointer_cast<::arrow::Decimal128Array>(batch->column(6));
  EXPECT_EQ(prices->FormatValue(0), "123.45");
  EXPECT_TRUE(prices->IsNull(1));
}

TEST(DataTaskReaderTest, BatchesSpanTasks) {
  auto schema = MakeIdSchema();
  ICEBERG_UNWRAP_OR_FAIL(auto first,
                         StaticDataTask::Make(*schema, schema, {MakeRow({int32_t{1}})}));
  ICEBERG_UNWRAP_OR_FAIL(
      auto second, StaticDataTask::Make(*schema, schema,
                                        {MakeRow({int32_t{2}}), MakeRow({int32_t{3}})}));

  ICEBERG_UNWRAP_OR_FAIL(auto stream,
                         ReadDataTasks(schema, {first, second}, /*batch_size=*/2));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 2);
  ASSERT_EQ(batches[0]->num_rows(), 2);
  ASSERT_EQ(batches[1]->num_rows(), 1);
  auto first_ids = std::static_pointer_cast<::arrow::Int32Array>(batches[0]->column(0));
  EXPECT_EQ(first_ids->Value(0), 1);
  EXPECT_EQ(first_ids->Value(1), 2);
  auto second_ids = std::static_pointer_cast<::arrow::Int32Array>(batches[1]->column(0));
  EXPECT_EQ(second_ids->Value(0), 3);
}

TEST(DataTaskReaderTest, NoTasksReturnsNoBatches) {
  ICEBERG_UNWRAP_OR_FAIL(auto stream, ReadDataTasks(MakeIdSchema(), {}));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  EXPECT_TRUE(batches.empty());
}

TEST(DataTaskReaderTest, RejectsInvalidArguments) {
  auto schema = MakeIdSchema();
  ICEBERG_UNWRAP_OR_FAIL(auto task, StaticDataTask::Make(*schema, schema, {}));

  EXPECT_THAT(ReadDataTasks(schema, {task}, /*batch_size=*/0),
              IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(ReadDataTasks(schema, {nullptr}), IsError(ErrorKind::kInvalidArgument));

  auto other_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
  EXPECT_THAT(ReadDataTasks(other_schema, {task}),
              HasErrorMessage("Data task schema does not match"));
}

TEST(DataTaskReaderTest, ReportsRowsThatDoNotMatchSchema) {
  auto schema = MakeIdSchema();
  ICEBERG_UNWRAP_OR_FAIL(
      auto task,
      StaticDataTask::Make(*schema, schema, {MakeRow({int32_t{1}, int32_t{2}})}));

  ICEBERG_UNWRAP_OR_FAIL(auto stream, ReadDataTasks(schema, {task}));
  EXPECT_THAT(ReadAllBatches(std::move(stream)),
              HasErrorMessage("Data task row has 2 fields but the schema has 1 columns"));
}

TEST(DataTaskReaderTest, RejectsNullRowStream) {
  auto schema = MakeIdSchema();
  auto task = std::make_shared<NullRowStreamTask>(schema);
  ICEBERG_UNWRAP_OR_FAIL(auto stream, ReadDataTasks(schema, {task}));
  EXPECT_THAT(ReadAllBatches(std::move(stream)),
              HasErrorMessage("Data task returned a null row stream"));
}

TEST(DataTaskReaderTest, RejectsNullRequiredFields) {
  EXPECT_THAT(
      ReadSingleField(SchemaField::MakeRequired(1, "id", int32()), std::monostate{}),
      HasErrorMessage("Required data task field id is null"));

  std::vector<std::pair<std::shared_ptr<Type>, Scalar>> cases{
      {std::make_shared<StructType>(std::vector<SchemaField>{}),
       std::shared_ptr<StructLike>{}},
      {std::make_shared<ListType>(SchemaField::MakeRequired(2, "element", int32())),
       std::shared_ptr<ArrayLike>{}},
      {std::make_shared<MapType>(SchemaField::MakeRequired(2, "key", string()),
                                 SchemaField::MakeOptional(3, "value", int32())),
       std::shared_ptr<MapLike>{}}};
  for (const auto& [type, value] : cases) {
    SCOPED_TRACE(type->ToString());
    EXPECT_THAT(ReadSingleField(SchemaField::MakeRequired(1, "field", type), value),
                HasErrorMessage("Required data task field field is null"));
    ICEBERG_UNWRAP_OR_FAIL(
        auto batches,
        ReadSingleField(SchemaField::MakeOptional(1, "field", type), value));
    ASSERT_EQ(batches.size(), 1);
    EXPECT_TRUE(batches[0]->column(0)->IsNull(0));
    EXPECT_TRUE(batches[0]->ValidateFull().ok());
  }
}

TEST(DataTaskReaderTest, RejectsNullRequiredNestedFields) {
  auto struct_type = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField::MakeRequired(2, "child", int32())});
  auto list_type =
      std::make_shared<ListType>(SchemaField::MakeRequired(2, "element", int32()));
  auto map_type =
      std::make_shared<MapType>(SchemaField::MakeRequired(2, "key", string()),
                                SchemaField::MakeRequired(3, "value", int32()));

  EXPECT_THAT(ReadSingleField(SchemaField::MakeOptional(1, "field", struct_type),
                              MakeRow({std::monostate{}})),
              HasErrorMessage("Required data task field child is null"));
  EXPECT_THAT(
      ReadSingleField(SchemaField::MakeOptional(1, "field", list_type),
                      std::make_shared<TestList>(std::vector<Scalar>{std::monostate{}})),
      HasErrorMessage("Required data task field element is null"));
  EXPECT_THAT(
      ReadSingleField(SchemaField::MakeOptional(1, "field", map_type),
                      std::make_shared<TestMap>(std::vector<std::pair<Scalar, Scalar>>{
                          {std::monostate{}, int32_t{1}}})),
      HasErrorMessage("Required data task field key is null"));
  EXPECT_THAT(
      ReadSingleField(SchemaField::MakeOptional(1, "field", map_type),
                      std::make_shared<TestMap>(std::vector<std::pair<Scalar, Scalar>>{
                          {std::string_view("k"), std::monostate{}}})),
      HasErrorMessage("Required data task field value is null"));
}

TEST(DataTaskReaderTest, RejectsMismatchedScalarAlternatives) {
  std::vector<std::pair<std::shared_ptr<Type>, Scalar>> cases{
      {int32(), true},
      {boolean(), int32_t{1}},
      {int32(), int64_t{1}},
      {int64(), int32_t{1}},
      {float32(), 1.0},
      {float64(), float{1.0}},
      {string(), int32_t{1}},
      {decimal(9, 2), int32_t{1}},
      {int32(), Decimal(1)},
      {std::make_shared<StructType>(std::vector<SchemaField>{}),
       std::make_shared<TestList>(std::vector<Scalar>{})},
      {std::make_shared<ListType>(SchemaField::MakeRequired(2, "element", int32())),
       MakeRow({})},
      {std::make_shared<MapType>(SchemaField::MakeRequired(2, "key", string()),
                                 SchemaField::MakeOptional(3, "value", int32())),
       std::make_shared<TestList>(std::vector<Scalar>{})}};
  for (const auto& [type, value] : cases) {
    SCOPED_TRACE(type->ToString());
    EXPECT_THAT(ReadSingleField(SchemaField::MakeOptional(1, "field", type), value),
                HasErrorMessage("does not match type"));
  }
}

TEST(DataTaskReaderTest, RejectsMismatchedNestedScalarAlternatives) {
  std::vector<std::pair<std::shared_ptr<Type>, Scalar>> cases{
      {std::make_shared<StructType>(
           std::vector<SchemaField>{SchemaField::MakeRequired(2, "child", int32())}),
       MakeRow({true})},
      {std::make_shared<ListType>(SchemaField::MakeRequired(2, "element", int32())),
       std::make_shared<TestList>(std::vector<Scalar>{true})},
      {std::make_shared<MapType>(SchemaField::MakeRequired(2, "key", int32()),
                                 SchemaField::MakeOptional(3, "value", int32())),
       std::make_shared<TestMap>(
           std::vector<std::pair<Scalar, Scalar>>{{true, int32_t{1}}})},
      {std::make_shared<MapType>(SchemaField::MakeRequired(2, "key", string()),
                                 SchemaField::MakeOptional(3, "value", int32())),
       std::make_shared<TestMap>(
           std::vector<std::pair<Scalar, Scalar>>{{std::string_view("k"), true}})}};
  for (const auto& [type, value] : cases) {
    SCOPED_TRACE(type->ToString());
    EXPECT_THAT(ReadSingleField(SchemaField::MakeRequired(1, "field", type), value),
                HasErrorMessage("does not match type"));
  }
}

TEST(StaticDataTaskTest, ProjectsTopLevelFields) {
  auto table_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                               SchemaField::MakeOptional(2, "name", string()),
                               SchemaField::MakeOptional(3, "flag", boolean())});
  auto projected_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(3, "flag", boolean()),
                               SchemaField::MakeRequired(1, "id", int32())});
  ICEBERG_UNWRAP_OR_FAIL(
      auto task,
      StaticDataTask::Make(*table_schema, projected_schema,
                           {MakeRow({int32_t{7}, std::string_view("a"), true})}));
  EXPECT_EQ(task->kind(), ScanTask::Kind::kDataTask);
  EXPECT_EQ(task->schema(), projected_schema);
  EXPECT_EQ(task->estimated_row_count(), 1);

  ICEBERG_UNWRAP_OR_FAIL(auto rows, task->Rows());
  ICEBERG_UNWRAP_OR_FAIL(auto all_rows, rows->ToVector());
  ASSERT_EQ(all_rows.size(), 1);
  ASSERT_EQ(all_rows[0]->num_fields(), 2);
  ICEBERG_UNWRAP_OR_FAIL(auto flag, all_rows[0]->GetField(0));
  EXPECT_EQ(std::get<bool>(flag), true);
  ICEBERG_UNWRAP_OR_FAIL(auto id, all_rows[0]->GetField(1));
  EXPECT_EQ(std::get<int32_t>(id), 7);
}

TEST(StaticDataTaskTest, RejectsPartialNestedProjection) {
  auto point_type = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField::MakeRequired(2, "x", float64()),
                               SchemaField::MakeRequired(3, "y", float64())});
  Schema table_schema(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "point", point_type)});
  auto projected_schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeOptional(1, "point",
                                std::make_shared<StructType>(std::vector<SchemaField>{
                                    SchemaField::MakeRequired(2, "x", float64())}))});

  EXPECT_THAT(StaticDataTask::Make(table_schema, projected_schema, {}),
              IsError(ErrorKind::kNotSupported));
}

TEST(StaticDataTaskTest, RejectsUnknownProjectedField) {
  auto schema = MakeIdSchema();
  auto projected_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(99, "missing", int32())});

  EXPECT_THAT(StaticDataTask::Make(*schema, projected_schema, {}),
              HasErrorMessage("Cannot find projected field missing (id 99)"));
}

}  // namespace iceberg
