// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#pragma once

#include <arrow/array.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/builder.h>
#include <arrow/memory_pool.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <glog/logging.h>
// project dependencies
#include "converter/column_converter.h"
#include "format/format_utils.h"
// starrocks dependencies
#include "column/column.h"
#include "column/column_helper.h"
#include "column/field.h"
#include "types/logical_type.h"
#include "util/json.h"
#include "util/slice.h"

namespace starrocks::lake::format {

constexpr int64_t MS_PER_DAY = 24 * 60 * 60 * 1000;
constexpr int64_t SECONDS_PER_DAY = 24 * 60 * 60;

template <arrow::Type::type ARROW_TYPE_ID, starrocks::LogicalType SR_TYPE,
          typename = std::enable_if<arrow::is_boolean_type<typename arrow::TypeIdTraits<ARROW_TYPE_ID>>::value ||
                                    arrow::is_number_type<typename arrow::TypeIdTraits<ARROW_TYPE_ID>>::value ||
                                    arrow::is_decimal_type<typename arrow::TypeIdTraits<ARROW_TYPE_ID>>::value>>
class PrimitiveConverter : public ColumnConverter {
    using ArrorType = typename arrow::TypeIdTraits<ARROW_TYPE_ID>::Type;
    using ArrowArrayType = typename arrow::TypeTraits<ArrorType>::ArrayType;
    using ArrowCType = typename arrow::TypeTraits<ArrorType>::CType;

    using SrColumnType = starrocks::RunTimeColumnType<SR_TYPE>;
    using SrCppType = starrocks::RunTimeCppType<SR_TYPE>;

    //
public:
    PrimitiveConverter(const std::shared_ptr<arrow::DataType> arrow_type,
                       const std::shared_ptr<starrocks::Field> sr_field, const arrow::MemoryPool* pool)
            : ColumnConverter(arrow_type, sr_field, pool){};

    arrow::Result<std::shared_ptr<arrow::Array>> toArrowArray(const std::shared_ptr<starrocks::Column>& column) {
        using ArrowBuilderType = typename arrow::TypeTraits<ArrorType>::BuilderType;

        std::unique_ptr<ArrowBuilderType> builder =
                std::make_unique<ArrowBuilderType>(_arrow_type, const_cast<arrow::MemoryPool*>(_pool));
        size_t num_rows = column->size();
        ARROW_RETURN_NOT_OK(builder->Reserve(num_rows));

        LOG(INFO) << "toArrowArray type name: " << _arrow_type->name() << " num rows " << num_rows;
        for (size_t i = 0; i < num_rows; ++i) {
            bool is_null = column->is_null(i);
            if (is_null) {
                ARROW_RETURN_NOT_OK(builder->AppendNull());
                continue;
            }

            auto* data_column = starrocks::ColumnHelper::get_data_column(column.get());
            const SrCppType* column_data = down_cast<const SrColumnType*>(data_column)->get_data().data();
            if constexpr (SR_TYPE == starrocks::LogicalType::TYPE_DATE ||
                          SR_TYPE == starrocks::LogicalType::TYPE_DATETIME) {
                if constexpr (std::is_base_of_v<arrow::Date32Type, ArrorType>) {
                    ARROW_RETURN_NOT_OK(builder->Append((column_data[i].to_unixtime() / MS_PER_DAY)));
                } else if constexpr (std::is_base_of_v<arrow::Date64Type, ArrorType>) {
                    ARROW_RETURN_NOT_OK(builder->Append(column_data[i].to_unixtime()));
                } else {
                    ARROW_RETURN_NOT_OK(builder->Append(column_data[i].to_unixtime()));
                }
            } else if constexpr (SR_TYPE == starrocks::LogicalType::TYPE_LARGEINT ||
                                 SR_TYPE == starrocks::LogicalType::TYPE_DECIMAL32 ||
                                 SR_TYPE == starrocks::LogicalType::TYPE_DECIMAL64 ||
                                 SR_TYPE == starrocks::LogicalType::TYPE_DECIMAL128) {
                int128_t c_value = column_data[i];
                int64_t high = c_value >> 64;
                uint64_t low = c_value;
                arrow::Decimal128 value(high, low);
                ARROW_RETURN_NOT_OK(builder->Append(value));
            } else {
                ARROW_RETURN_NOT_OK(builder->Append((column_data[i])));
            }
        }
        // // copy null bitmap
        // if (column->is_nullable()) {
        //     auto nullable = down_cast<NullableColumn*>(column.get());
        //     builder->AppendToBitmap(nullable->null_column_data().data(), num_rows);
        //     builder->UnsafeAppendToBitmap(nullable->null_column_data());
        // }

        std::shared_ptr<arrow::Array> out;
        auto result = builder->Finish(&out);
        LOG(INFO) << "toArrowArray arrow column num " << (out)->length() << " contenxt s " << (out)->ToString();
        return out;
    }

    arrow::Status toSrColumn(const std::shared_ptr<arrow::Array> array, std::shared_ptr<starrocks::Column>& out) {
        if (!out->is_nullable() && array->null_count() > 0) {
            return arrow::Status::Invalid("Can not convert array which contains null to non-nullable column!");
        }
        auto num_rows = array->length();
        out->resize(num_rows);
        // copy data column
        const auto& real_array = arrow::internal::checked_pointer_cast<const ArrowArrayType>(array);
        const auto data_column = arrow::internal::checked_pointer_cast<SrColumnType>(get_data_column(out));
        if constexpr (SR_TYPE == starrocks::LogicalType::TYPE_DATE ||
                      SR_TYPE == starrocks::LogicalType::TYPE_DATETIME) {
            for (size_t i = 0; i < num_rows; ++i) {
                if constexpr (std::is_base_of_v<arrow::Date32Type, ArrorType>) {
                    ArrowCType arrow_value = real_array->Value(i);
                    TimestampValue ts;
                    int64_t seconds = arrow_value * SECONDS_PER_DAY;
                    ts.from_unixtime(seconds, cctz::local_time_zone());
                    DateValue value = (DateValue)ts;
                    data_column->append_datum(Datum(value));
                } else if constexpr (std::is_base_of_v<arrow::Date64Type, ArrorType>) {
                    ArrowCType arrow_value = real_array->Value(i);
                    TimestampValue ts;
                    int64_t seconds = arrow_value / 1000;
                    ts.from_unixtime(seconds, cctz::local_time_zone());
                    DateValue value = (DateValue)ts;
                    data_column->append_datum(Datum(value));
                } else {
                    ArrowCType arrow_value = real_array->Value(i);
                    int64_t seconds = arrow_value / 1000;
                    TimestampValue value;
                    value.from_unixtime(seconds, TimezoneUtils::local_time_zone());
                    data_column->append_datum(Datum(value));
                }
            }
        } else if constexpr (SR_TYPE == starrocks::LogicalType::TYPE_LARGEINT ||
                             SR_TYPE == starrocks::LogicalType::TYPE_DECIMAL32 ||
                             SR_TYPE == starrocks::LogicalType::TYPE_DECIMAL64 ||
                             SR_TYPE == starrocks::LogicalType::TYPE_DECIMAL128) {
            for (size_t i = 0; i < num_rows; ++i) {
                SrCppType value;
                arrow::Decimal128 arrow_value(real_array->GetValue(i));
                if constexpr (SR_TYPE == starrocks::LogicalType::TYPE_DECIMAL32 ||
                              SR_TYPE == starrocks::LogicalType::TYPE_DECIMAL64) {
                    ARROW_RETURN_NOT_OK(arrow_value.ToInteger(&value));
                    data_column->append_datum(value);
                } else {
                    value = arrow_value.high_bits();
                    value = value << 64 | arrow_value.low_bits();
                }
                data_column->append_datum(value);
            }
        } else if constexpr (SR_TYPE == starrocks::LogicalType::TYPE_BOOLEAN) {
            // arrow boolean use bitmap to storage the true/false,
            // so we can not copy memory directly.
            for (size_t i = 0; i < num_rows; ++i) {
                data_column->get_data()[i] = real_array->IsNull(i);
            }
        } else {
            const ArrowCType* array_data = real_array->raw_values();
            SrCppType* data = data_column->get_data().data();
            memcpy(data, array_data, num_rows * sizeof(SrCppType));
        }

        // copy null bitmap
        if (out->is_nullable()) {
            auto nullable = down_cast<NullableColumn*>(out.get());
            for (size_t i = 0; i < num_rows; ++i) {
                nullable->null_column_data()[i] = array->IsNull(i);
            }
            nullable->set_has_null(true);
        }
        return arrow::Status::OK();
    }
};

} // namespace starrocks::lake::format