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

package com.starrocks.http.rest.v2.vo;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ColumnView {

    @SerializedName("name")
    private String name;

    @SerializedName("type")
    private TypeView type;

    @SerializedName("aggregationType")
    private String aggregationType;

    @SerializedName("isKey")
    private Boolean key;

    @SerializedName("isAllowNull")
    private Boolean allowNull;

    @SerializedName("isAutoIncrement")
    private Boolean autoIncrement;

    @SerializedName("defaultValueType")
    private String defaultValueType;

    @SerializedName("defaultValue")
    private String defaultValue;

    @SerializedName("defaultExpr")
    private String defaultExpr;

    @SerializedName("comment")
    private String comment;

    @SerializedName("uniqueId")
    private Integer uniqueId;

    public ColumnView() {
    }

    /**
     * Create from {@link Column}
     */
    public static ColumnView createFrom(Column column) {
        ColumnView cvo = new ColumnView();
        cvo.setName(column.getName());

        Optional.ofNullable(column.getType())
                .ifPresent(type -> cvo.setType(TypeView.toTypeView(type)));

        Optional.ofNullable(column.getAggregationType())
                .ifPresent(aggType -> cvo.setAggregationType(aggType.toSql()));

        cvo.setKey(column.isKey());
        cvo.setAllowNull(column.isAllowNull());
        cvo.setAutoIncrement(column.isAutoIncrement());
        cvo.setDefaultValueType(column.getDefaultValueType().name());
        cvo.setDefaultValue(column.getDefaultValue());

        Optional.ofNullable(column.getDefaultExpr())
                .ifPresent(defaultExpr -> cvo.setDefaultExpr(defaultExpr.getExpr()));

        cvo.setComment(column.getComment());
        cvo.setUniqueId(column.getUniqueId());
        return cvo;
    }

    private abstract static class TypeView {

        @SerializedName("name")
        protected String name;

        static TypeView toTypeView(Type type) {
            TypeView tvo;
            if (type instanceof ScalarType) {
                tvo = ScalarTypeView.createFrom((ScalarType) type);
            } else if (type instanceof ArrayType) {
                tvo = ArrayTypeView.createFrom((ArrayType) type);
                tvo.name = "ARRAY";

            } else if (type instanceof StructType) {
                tvo = StructTypeView.createFrom((StructType) type);
                tvo.name = "STRUCT";
            } else if (type instanceof MapType) {
                tvo = MapTypeView.createFrom((MapType) type);
                tvo.name = "MAP";
            } else {
                throw new IllegalStateException("Unsupported item type: " + type);
            }
            return tvo;
        }

    }

    private static class ScalarTypeView extends TypeView {

        @SerializedName("typeSize")
        private Integer typeSize;

        @SerializedName("columnSize")
        private Integer columnSize;

        @SerializedName("precision")
        private Integer precision;

        @SerializedName("scale")
        private Integer scale;

        public ScalarTypeView() {
        }

        /**
         * Create from {@link ScalarType}
         */
        public static ScalarTypeView createFrom(ScalarType scalarType) {
            ScalarTypeView stvo = new ScalarTypeView();
            PrimitiveType priType = scalarType.getPrimitiveType();
            stvo.name = priType.toString();
            stvo.typeSize = scalarType.getTypeSize();
            stvo.columnSize = scalarType.getColumnSize();
            stvo.precision = scalarType.getPrecision();
            stvo.scale = scalarType.getScalarScale();
            return stvo;
        }

        public String getName() {
            return name;
        }

        public Integer getTypeSize() {
            return typeSize;
        }

        public Integer getColumnSize() {
            return columnSize;
        }

        public Integer getPrecision() {
            return precision;
        }

        public Integer getScale() {
            return scale;
        }
    }

    private static class ArrayTypeView extends TypeView {

        @SerializedName("itemType")
        private TypeView itemType;

        public ArrayTypeView() {
        }

        /**
         * Create from {@link ArrayType}
         */
        public static ArrayTypeView createFrom(ArrayType arrayType) {
            ArrayTypeView atvo = new ArrayTypeView();
            atvo.itemType = toTypeView(arrayType.getItemType());
            return atvo;
        }

        public TypeView getItemType() {
            return itemType;
        }
    }

    private static class StructTypeView extends TypeView {

        @SerializedName("named")
        private Boolean named;

        @SerializedName("fields")
        private List<StructFieldView> fields;

        public StructTypeView() {
        }

        /**
         * Create from {@link StructType}
         */
        public static StructTypeView createFrom(StructType structType) {
            StructTypeView stvo = new StructTypeView();
            stvo.named = structType.isNamed();

            List<StructField> fields = structType.getFields();
            if (CollectionUtils.isEmpty(fields)) {
                return stvo;
            }

            stvo.fields = fields.stream()
                    .map(StructFieldView::createFrom)
                    .collect(Collectors.toList());

            return stvo;
        }

        private static class StructFieldView {

            @SerializedName("name")
            private String name;

            @SerializedName("type")
            private TypeView type;

            public StructFieldView() {
            }

            /**
             * Create from {@link StructField}
             */
            public static StructFieldView createFrom(StructField structField) {
                StructFieldView sfvo = new StructFieldView();
                sfvo.name = structField.getName();
                sfvo.type = toTypeView(structField.getType());
                return sfvo;
            }

            public String getName() {
                return name;
            }

            public TypeView getType() {
                return type;
            }
        }

        public Boolean getNamed() {
            return named;
        }

        public List<StructFieldView> getFields() {
            return fields;
        }
    }

    private static class MapTypeView extends TypeView {

        @SerializedName("keyType")
        private TypeView keyType;

        @SerializedName("valueType")
        private TypeView valueType;

        public MapTypeView() {
        }

        /**
         * Create from {@link MapType}
         */
        public static MapTypeView createFrom(MapType mapType) {
            MapTypeView mtvo = new MapTypeView();
            mtvo.keyType = toTypeView(mapType.getKeyType());
            mtvo.valueType = toTypeView(mapType.getValueType());
            return mtvo;
        }

        public TypeView getKeyType() {
            return keyType;
        }

        public TypeView getValueType() {
            return valueType;
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public TypeView getType() {
        return type;
    }

    public void setType(TypeView type) {
        this.type = type;
    }

    public String getAggregationType() {
        return aggregationType;
    }

    public void setAggregationType(String aggregationType) {
        this.aggregationType = aggregationType;
    }

    public Boolean getKey() {
        return key;
    }

    public void setKey(Boolean key) {
        this.key = key;
    }

    public Boolean getAllowNull() {
        return allowNull;
    }

    public void setAllowNull(Boolean allowNull) {
        this.allowNull = allowNull;
    }

    public Boolean getAutoIncrement() {
        return autoIncrement;
    }

    public void setAutoIncrement(Boolean autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

    public String getDefaultValueType() {
        return defaultValueType;
    }

    public void setDefaultValueType(String defaultValueType) {
        this.defaultValueType = defaultValueType;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public String getDefaultExpr() {
        return defaultExpr;
    }

    public void setDefaultExpr(String defaultExpr) {
        this.defaultExpr = defaultExpr;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public Integer getUniqueId() {
        return uniqueId;
    }

    public void setUniqueId(Integer uniqueId) {
        this.uniqueId = uniqueId;
    }
}
