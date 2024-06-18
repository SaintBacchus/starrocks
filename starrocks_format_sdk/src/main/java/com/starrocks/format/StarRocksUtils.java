// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package com.starrocks.format;

import com.starrocks.format.rest.model.Column;
import com.starrocks.format.rest.model.MaterializedIndexMeta;
import com.starrocks.format.rest.model.TableSchema;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class StarRocksUtils {

    public static final String STARROKCS_TABLE_ID = "starrokcs.table.id";
    public static final String STARROKCS_TABLE_KEY_TYPE = "starrokcs.table.keyType";
    public static final String STARROKCS_TABLE_COMPRESSION =  "starrokcs.compression";
    public static final String STARROKCS_TABLE_KEY_INDEX = "starrokcs.table.keyIndex";
    public static final String STARROKCS_TABLE_SHORT_KEY_NUM = "starrokcs.table.shortKeyNum";
    public static final String STARROKCS_TABLE_NUMROWS_PER_BLOCK = "starrokcs.table.numRowsPerBlock";
    public static final String STARROKCS_COLUMN_ID = "starrokcs.column.id";
    public static final String STARROKCS_COLUMN_TYPE = "starrokcs.column.type";
    public static final String STARROKCS_COLUMN_IS_KEY = "starrokcs.column.isKey";
    public static final String STARROKCS_COLUMN_MAX_LENGTH = "starrokcs.column.maxLength";
    public static final String STARROKCS_COLUMN_AGGREGATION_TYPE = "starrokcs.column.aggregationType";
    public static final String STARROKCS_COLUMN_IS_AUTO_INCREMENT = "starrokcs.column.IsAutoIncrement";

    public static Schema toArrowSchema(TableSchema tableSchema) {
        MaterializedIndexMeta indexMeta = tableSchema.getIndexMetas().get(0);
        Map<String, String> metadata = new HashMap<>();
        metadata.put(STARROKCS_TABLE_ID, String.valueOf(indexMeta.getIndexId()));
        metadata.put(STARROKCS_TABLE_KEY_TYPE, indexMeta.getKeysType());
        List<Field> fields = indexMeta.getColumns().stream().map(StarRocksUtils::toArrowField).collect(Collectors.toList());
        return new Schema(fields, metadata);
    }

    public static Field toArrowField(Column column) {
        ArrowType arrowType = toArrowType(column.getType().getName(),
                Optional.ofNullable(column.getType().getPrecision()).orElse(0),
                Optional.ofNullable(column.getType().getScale()).orElse(0));
        Map<String, String> metadata = new HashMap<>();
        metadata.put(STARROKCS_COLUMN_ID, String.valueOf(column.getUniqueId()));
        metadata.put(STARROKCS_COLUMN_TYPE, column.getType().getName());
        metadata.put(STARROKCS_COLUMN_IS_KEY, String.valueOf(column.getKey()));
        metadata.put(STARROKCS_COLUMN_MAX_LENGTH, String.valueOf(column.getType().getColumnSize()));
        metadata.put(STARROKCS_COLUMN_AGGREGATION_TYPE, Optional.ofNullable(column.getAggregationType()).orElse("NONE"));
        metadata.put(STARROKCS_COLUMN_IS_AUTO_INCREMENT, String.valueOf(column.getAutoIncrement()));

        List<Field> children = getChildren(column.getType());
        return new Field(column.getName(),
                new FieldType(Optional.ofNullable(column.getAllowNull()).orElse(false), arrowType, null, metadata),
                children);
    }

    public static Field toArrowField(String fieldName, Column.Type columnType) {
        ArrowType arrowType = toArrowType(columnType.getName(),
                Optional.ofNullable(columnType.getPrecision()).orElse(0),
                Optional.ofNullable(columnType.getScale()).orElse(0));
        Map<String, String> metadata = new HashMap<>();
        metadata.put(STARROKCS_COLUMN_TYPE, columnType.getName());
        metadata.put(STARROKCS_COLUMN_MAX_LENGTH, String.valueOf(columnType.getColumnSize()));
        List<Field> children = getChildren(columnType);
        return new Field(fieldName, new FieldType(false, arrowType, null, metadata), children);
    }

    public static List<Field> getChildren(Column.Type columnType) {
        List<Field> children = new ArrayList<>();
        if (!columnType.isScalar()) {
            if ("MAP".equals(columnType.getName())) {
                Field keyField = toArrowField("key", columnType.getKeyType());
                Field valueField = toArrowField("value", columnType.getValueType());
                Map<String, String> metadata = new HashMap<>();
                metadata.put(STARROKCS_COLUMN_TYPE, "STRUCT");
                Field childField = new Field("entries",
                        new FieldType(false, ArrowType.Struct.INSTANCE, null, metadata), Arrays.asList(keyField, valueField));
                children.add(childField);
            } else if ("ARRAY".equals(columnType.getName())) {
                Field itemField = toArrowField("item1", columnType.getItemType());
                children.add(itemField);
            } else if ("STRUCT".equals(columnType.getName())) {
                for (Column child : columnType.getFields()) {
                    Field childField = toArrowField(child);
                    children.add(childField);
                }
            }
        }
        return children;
    }



    public static ArrowType toArrowType(String srType, int precision, int scale) {
        if (DataType.isUnsupported(srType)) {
            throw new UnsupportedOperationException("Unsupported column type: " + srType);
        }
        ArrowType arrowType;
        switch (srType) {
            case "BOOLEAN":
                arrowType = ArrowType.Bool.INSTANCE;
                break;
            case "TINYINT":
                arrowType = new ArrowType.Int(8, true);
                break;
            case "SMALLINT":
                arrowType = new ArrowType.Int(16, true);
                break;
            case "INT":
                arrowType = new ArrowType.Int(32, true);
                break;
            case "BIGINT":
                arrowType = new ArrowType.Int(64, true);
                break;
            case "LARGEINT":
                arrowType = new ArrowType.Decimal(38, 0, 128);
                break;
            case "FLOAT":
                arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
                break;
            case "DOUBLE":
                arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
                break;
            case "DECIMAL":
                arrowType = new ArrowType.Decimal(precision, scale, 128);
                break;
            case "DECIMAL32":
                arrowType = new ArrowType.Decimal(precision, scale, 128);
                break;
            case "DECIMAL64":
                arrowType = new ArrowType.Decimal(precision, scale, 128);
                break;
            case "DECIMAL128":
                arrowType = new ArrowType.Decimal(precision, scale, 128);
                break;
            case "CHAR":
            case "VARCHAR":
            case "JSON":
                arrowType = ArrowType.Utf8.INSTANCE;
                break;
            case "DATE":
                arrowType = new ArrowType.Date(DateUnit.DAY);
                break;
            case "DATETIME":
                arrowType = new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
                break;
            case "OBJECT":
            case "BITMAP":
            case "HLL":
            case "BINARY":
            case "VARBINARY":
                arrowType = ArrowType.Binary.INSTANCE;
                break;
            case "MAP":
                arrowType = new ArrowType.Map(false);
                break;
            case "ARRAY":
                arrowType = ArrowType.List.INSTANCE;
                break;
            case "STRUCT":
                arrowType = ArrowType.Struct.INSTANCE;
                break;
            default:
                throw new IllegalArgumentException("Unsupported type: " + srType);
        }
        return arrowType;
    }

}
