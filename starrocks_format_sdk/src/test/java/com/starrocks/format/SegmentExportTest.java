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

import com.google.gson.Gson;
import com.starrocks.format.rest.LoadNonSupportException;
import com.starrocks.format.rest.RequestException;
import com.starrocks.format.rest.Validator;
import com.starrocks.format.rest.model.TablePartition;
import com.starrocks.format.rest.model.TableSchema;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class SegmentExportTest extends BaseFormatTest {
    @BeforeAll
    public static void init() throws Exception {
        BaseFormatTest.init();
        settings.setSegmentExportEnabled("share_nothing");
    }

    private static Stream<Arguments> testAllPrimitiveType() {
        return Stream.of(
                Arguments.of("tb_all_primitivetype_write_duplicate"),
                Arguments.of("tb_all_primitivetype_write_unique"),
                Arguments.of("tb_all_primitivetype_write_aggregate"),
                Arguments.of("tb_all_primitivetype_write_primary")
        );
    }

    private static Stream<Arguments> testAllPrimitiveTypeTwice() {
        return Stream.of(
                Arguments.of("tb_all_primitivetype_write_duplicate2"),
                Arguments.of("tb_all_primitivetype_write_unique2"),
                Arguments.of("tb_all_primitivetype_write_aggregate2"),
                Arguments.of("tb_all_primitivetype_write_primary2")
        );
    }

    private static Stream<Arguments> testSchemaChangeTable() {
        return Stream.of(
                Arguments.of("tb_fast_schema_change_table"),
                Arguments.of("tb_no_fast_schema_change_table")
        );
    }

    private static Stream<Arguments> testCompressTypeTable() {
        return Stream.of(
                Arguments.of("tb_all_primitivetype_compress_lz4"),
                Arguments.of("tb_all_primitivetype_compress_zstd"),
                Arguments.of("tb_all_primitivetype_compress_zlib"),
                Arguments.of("tb_all_primitivetype_compress_snappy")
        );
    }

    private static Stream<Arguments> testSortByTypeTable() {
        return Stream.of(
                Arguments.of("tb_all_primitivetype_order_by_int"),
                Arguments.of("tb_all_primitivetype_order_by_varchar")
        );
    }

    private static Stream<Arguments> testSwapTable() {
        return Stream.of(
                Arguments.of("tb_all_primitivetype_swap_table"),
                Arguments.of("tb_all_primitivetype_swap_diff_table")
        );
    }

    private static Stream<Arguments> testStorageTypeTable() {
        return Stream.of(
                Arguments.of("tb_all_primitivetype_storage_hdd")
        );
    }


    private static Stream<Arguments> testPartitionTable() {
        return Stream.of(
                Arguments.of("tb_all_primitivetype_partition_dup_table"),
                Arguments.of("tb_all_primitivetype_partition_primary_table")
        );
    }

    private static Stream<Arguments> testListPartitionTable() {
        return Stream.of(
                Arguments.of("tb_all_primitivetype_list_partition_dup_table"),
                Arguments.of("tb_all_primitivetype_list_partition_primary_table")
        );
    }

    @ParameterizedTest
    @MethodSource("testAllPrimitiveType")
    public void testAllPrimitiveType(String tableName) throws Exception {
        testTableTypes(tableName);
    }

    @ParameterizedTest
    @MethodSource("testAllPrimitiveTypeTwice")
    public void testAllPrimitiveTypeTwice(String tableName) throws Exception {
        int times = 2;
        for (int i = 0; i < times; i++) {
            String uuid = RandomStringUtils.randomAlphabetic(8);
            String stageDir = "s3a://bucket1/.staging_ut/" + uuid + "/";
            TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, dbName, tableName);
            Validator.validateSegmentLoadExport(tableSchema);
            Schema tabletSchema = StarRocksUtils.toArrowSchema(tableSchema);

            List<TablePartition> partitions = restClient.listTablePartitions(DEFAULT_CATALOG, dbName,
                    tableName, false);
            long tableId = tableSchema.getId();
            long indexId = tableSchema.getIndexMetas().get(0).getIndexId();

            assertFalse(partitions.isEmpty());
            for (TablePartition partition : partitions) {
                List<TablePartition.Tablet> tablets = partition.getTablets();
                assertFalse(tablets.isEmpty());

                for (TablePartition.Tablet tablet : tablets) {
                    Long tabletId = tablet.getId();
                    try {
                        String storagePath = stageDir + "/" + tableId + "/" + partition.getId() + "/"
                                + indexId + "/" + tabletId;
                        Map<String, String> options = settings.toMap();
                        String metaUrl = tablet.getMetaUrls().get(0);
                        String metaContext = restClient.getTabletMeta(metaUrl);
                        options.put("starrocks.format.metaContext", metaContext);
                        StarRocksWriter writer = new StarRocksWriter(tabletId,
                                -1L,
                                tabletSchema,
                                storagePath,
                                options);
                        writer.open();
                        // write use arrow interface
                        try (VectorSchemaRoot vsr = VectorSchemaRoot.create(tabletSchema, writer.getRootAllocator())) {
                            fillSampleData(vsr, 0, 200);
                            writer.write(vsr);
                            vsr.clear();

                            fillSampleData(vsr, 200, 200);
                            writer.write(vsr);
                            vsr.clear();
                        }
                        writer.flush();
                        writer.finish();
                        writer.close();
                        writer.release();
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail();
                    }
                }
            }

            String label = String.format("bypass_write_%s_%s_%s", dbName, tableName, uuid);
            String ak = settings.getS3AccessKey();
            String sk = settings.getS3SecretKey();
            String endpoint = settings.getS3Endpoint();

            loadSegmentData(dbName, label, stageDir, tableName, ak, sk, endpoint);
            boolean res = waitUtilLoadFinished(dbName, label);
            assertTrue(res);
        }

        List<Map<String, String>> outputs = getTableRowNum(dbName, tableName);
        assertEquals(1, outputs.size());
        switch (tableName) {
            case "tb_all_primitivetype_write_duplicate2":
                assertEquals(800, Integer.valueOf(outputs.get(0).get("num")));
                break;
            case "tb_all_primitivetype_write_unique2":
                assertEquals(400, Integer.valueOf(outputs.get(0).get("num")));
                break;
            case "tb_all_primitivetype_write_aggregate2":
                assertEquals(400, Integer.valueOf(outputs.get(0).get("num")));
                break;
            case "tb_all_primitivetype_write_primary2":
                assertEquals(400, Integer.valueOf(outputs.get(0).get("num")));
                break;
            default:
                fail();
        }

        List<Map<String, String>> outputsNotNull = getTableRowNumNotNUll(dbName, tableName);
        assertEquals(1, outputsNotNull.size());
        switch (tableName) {
            case "tb_all_primitivetype_write_duplicate2":
                assertEquals(798, Integer.valueOf(outputsNotNull.get(0).get("num")));
                break;
            case "tb_all_primitivetype_write_unique2":
                assertEquals(399, Integer.valueOf(outputsNotNull.get(0).get("num")));
                break;
            case "tb_all_primitivetype_write_aggregate2":
                assertEquals(399, Integer.valueOf(outputsNotNull.get(0).get("num")));
                break;
            case "tb_all_primitivetype_write_primary2":
                assertEquals(399, Integer.valueOf(outputsNotNull.get(0).get("num")));
                break;
            default:
                fail();
        }
    }

    @ParameterizedTest
    @MethodSource("testSchemaChangeTable")
    public void testSchemaChangeTable(String tableName) throws Exception {
        String dropColumnTable = String.format("alter table `demo`.`%s` DROP COLUMN c_bigint",tableName);
        executeSql(dropColumnTable);
        Assertions.assertTrue(waitAlterTableColumnFinished(tableName));

        String addColumnTable = String.format("alter table `demo`.`%s` ADD COLUMN new_col INT" +
                " DEFAULT \"0\" AFTER c_datetime", tableName);
        executeSql(addColumnTable);
        Assertions.assertTrue(waitAlterTableColumnFinished(tableName));
        if (tableName.equalsIgnoreCase("tb_fast_schema_change_table")) {
            testTableTypes(tableName, false);
        } else {
            testTableTypes(tableName);
        }
    }

    @Test
    public void testRenameTable() throws Exception {
        String tableName = "tb_all_primitivetype_rename_table";

        String uuid = RandomStringUtils.randomAlphabetic(8);
        String stageDir = "s3a://bucket1/.staging_ut/" + uuid + "/";
        TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, dbName, tableName);
        Validator.validateSegmentLoadExport(tableSchema);
        Schema tabletSchema = StarRocksUtils.toArrowSchema(tableSchema);

        List<TablePartition> partitions = restClient.listTablePartitions(DEFAULT_CATALOG, dbName, tableName, false);
        long tableId = tableSchema.getId();
        long indexId = tableSchema.getIndexMetas().get(0).getIndexId();

        assertFalse(partitions.isEmpty());

        for (TablePartition partition : partitions) {
            List<TablePartition.Tablet> tablets = partition.getTablets();
            assertFalse(tablets.isEmpty());

            for (TablePartition.Tablet tablet : tablets) {
                Long tabletId = tablet.getId();
                try {
                    String storagePath = stageDir + "/" + tableId + "/" + partition.getId() + "/"
                            + indexId + "/" + tabletId;
                    Map<String, String> options = settings.toMap();
                    String metaUrl = tablet.getMetaUrls().get(0);
                    String metaContext = restClient.getTabletMeta(metaUrl);
                    options.put("starrocks.format.metaContext", metaContext);
                    StarRocksWriter writer = new StarRocksWriter(tabletId,
                            -1L,
                            tabletSchema,
                            storagePath,
                            options);
                    writer.open();
                    // write use arrow interface
                    try (VectorSchemaRoot vsr = VectorSchemaRoot.create(tabletSchema, writer.getRootAllocator())) {
                        fillSampleData(vsr, 0, 200);
                        writer.write(vsr);
                        vsr.clear();

                        fillSampleData(vsr, 200, 200);
                        writer.write(vsr);
                        vsr.clear();
                    }
                    writer.flush();
                    writer.finish();
                    writer.close();
                    writer.release();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail();
                }
            }
        }

        String renameSql = String.format("alter table `demo`.`%s` rename tb_all_primitivetype_rename_table2",tableName);
        executeSql(renameSql);
        Thread.sleep(1000);

        String label = String.format("bypass_write_%s_%s_%s", dbName, tableName, uuid);
        String ak = settings.getS3AccessKey();
        String sk = settings.getS3SecretKey();
        String endpoint = settings.getS3Endpoint();

        IllegalStateException illegalStateException = Assertions.assertThrows(IllegalStateException.class,
                ()->loadSegmentData(dbName, label, stageDir, tableName, ak, sk, endpoint));
        Assertions.assertEquals("submit sql error, Getting analyzing error." +
                " Detail message: Table demo.tb_all_primitivetype_rename_table is not found.",
                illegalStateException.getMessage());
    }

    @Test
    public void testModifyTableComment() throws Exception {
        String tableName = "tb_all_primitivetype_modify_comment";

        String uuid = RandomStringUtils.randomAlphabetic(8);
        String stageDir = "s3a://bucket1/.staging_ut/" + uuid + "/";
        TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, dbName, tableName);
        Validator.validateSegmentLoadExport(tableSchema);
        Schema tabletSchema = StarRocksUtils.toArrowSchema(tableSchema);

        List<TablePartition> partitions = restClient.listTablePartitions(DEFAULT_CATALOG, dbName, tableName, false);
        long tableId = tableSchema.getId();
        long indexId = tableSchema.getIndexMetas().get(0).getIndexId();

        assertFalse(partitions.isEmpty());

        for (TablePartition partition : partitions) {
            List<TablePartition.Tablet> tablets = partition.getTablets();
            assertFalse(tablets.isEmpty());

            for (TablePartition.Tablet tablet : tablets) {
                Long tabletId = tablet.getId();
                try {
                    String storagePath = stageDir + "/" + tableId + "/" + partition.getId() + "/"
                            + indexId + "/" + tabletId;
                    Map<String, String> options = settings.toMap();
                    String metaUrl = tablet.getMetaUrls().get(0);
                    String metaContext = restClient.getTabletMeta(metaUrl);
                    options.put("starrocks.format.metaContext", metaContext);
                    StarRocksWriter writer = new StarRocksWriter(tabletId,
                            -1L,
                            tabletSchema,
                            storagePath,
                            options);
                    writer.open();
                    // write use arrow interface
                    try (VectorSchemaRoot vsr = VectorSchemaRoot.create(tabletSchema, writer.getRootAllocator())) {
                        fillSampleData(vsr, 0, 200);
                        writer.write(vsr);
                        vsr.clear();

                        fillSampleData(vsr, 200, 200);
                        writer.write(vsr);
                        vsr.clear();
                    }
                    writer.flush();
                    writer.finish();
                    writer.close();
                    writer.release();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail();
                }
            }
        }

        String modifyComment = String.format("alter table `demo`.`%s` COMMENT = \"this is test table\"",tableName);
        executeSql(modifyComment);
        Thread.sleep(1000);

        String label = String.format("bypass_write_%s_%s_%s", dbName, tableName, uuid);
        String ak = settings.getS3AccessKey();
        String sk = settings.getS3SecretKey();
        String endpoint = settings.getS3Endpoint();

        loadSegmentData(dbName, label, stageDir, tableName, ak, sk, endpoint);
        boolean res = waitUtilLoadFinished(dbName, label);
        assertTrue(res);

        List<Map<String, String>> outputs = getTableRowNum(dbName, tableName);
        assertEquals(1, outputs.size());
        assertEquals(400, Integer.valueOf(outputs.get(0).get("num")));

        List<Map<String, String>> outputsNotNull = getTableRowNumNotNUll(dbName, tableName);
        assertEquals(1, outputsNotNull.size());
        assertEquals(399, Integer.valueOf(outputsNotNull.get(0).get("num")));
    }

    @ParameterizedTest
    @MethodSource("testSwapTable")
    public void testSwapTable(String tableName) throws Exception {
        String swapTableName = "tb_all_primitivetype_swap_table" + "1";

        String uuid = RandomStringUtils.randomAlphabetic(8);
        String stageDir = "s3a://bucket1/.staging_ut/" + uuid + "/";
        TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, dbName, tableName);
        Validator.validateSegmentLoadExport(tableSchema);
        Schema tabletSchema = StarRocksUtils.toArrowSchema(tableSchema);

        List<TablePartition> partitions = restClient.listTablePartitions(DEFAULT_CATALOG, dbName, tableName, false);
        long tableId = tableSchema.getId();
        long indexId = tableSchema.getIndexMetas().get(0).getIndexId();

        assertFalse(partitions.isEmpty());

        for (TablePartition partition : partitions) {
            List<TablePartition.Tablet> tablets = partition.getTablets();
            assertFalse(tablets.isEmpty());

            for (TablePartition.Tablet tablet : tablets) {
                Long tabletId = tablet.getId();
                try {
                    String storagePath = stageDir + "/" + tableId + "/" + partition.getId() + "/"
                            + indexId + "/" + tabletId;
                    Map<String, String> options = settings.toMap();
                    String metaUrl = tablet.getMetaUrls().get(0);
                    String metaContext = restClient.getTabletMeta(metaUrl);
                    options.put("starrocks.format.metaContext", metaContext);
                    StarRocksWriter writer = new StarRocksWriter(tabletId,
                            -1L,
                            tabletSchema,
                            storagePath,
                            options);
                    writer.open();
                    // write use arrow interface
                    try (VectorSchemaRoot vsr = VectorSchemaRoot.create(tabletSchema, writer.getRootAllocator())) {
                        fillSampleData(vsr, 0, 200);
                        writer.write(vsr);
                        vsr.clear();

                        fillSampleData(vsr, 200, 200);
                        writer.write(vsr);
                        vsr.clear();
                    }
                    writer.flush();
                    writer.finish();
                    writer.close();
                    writer.release();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail();
                }
            }
        }

        String renameSql = String.format("alter table `demo`.`%s` swap with %s",tableName, swapTableName);
        executeSql(renameSql);
        Thread.sleep(1000);

        String label = String.format("bypass_write_%s_%s_%s", dbName, tableName, uuid);
        String ak = settings.getS3AccessKey();
        String sk = settings.getS3SecretKey();
        String endpoint = settings.getS3Endpoint();

        loadSegmentData(dbName, label, stageDir, tableName, ak, sk, endpoint);
        boolean res = waitUtilLoadFinished(dbName, label);
        assertFalse(res);
    }

    @ParameterizedTest
    @MethodSource("testCompressTypeTable")
    public void testCompressTypeTable(String tableName) throws Exception {
        testTableTypes(tableName);
    }

    @ParameterizedTest
    @MethodSource("testSortByTypeTable")
    public void testSortByTypeTable(String tableName) throws Exception {
        testTableTypes(tableName);
    }

    @ParameterizedTest
    @MethodSource("testStorageTypeTable")
    public void testStorageTypeTable(String tableName) throws Exception {
        testTableTypes(tableName);
    }
    private void testTableTypes(String tableName) throws LoadNonSupportException, RequestException {
        testTableTypes(tableName, true);
    }

    private void testTableTypes(String tableName, boolean assertOK) throws LoadNonSupportException, RequestException {
        String uuid = RandomStringUtils.randomAlphabetic(8);
        String stageDir = "s3a://bucket1/.staging_ut/" + uuid + "/";
        TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, dbName, tableName);
        Validator.validateSegmentLoadExport(tableSchema);
        Schema tabletSchema = StarRocksUtils.toArrowSchema(tableSchema);

        List<TablePartition> partitions = restClient.listTablePartitions(DEFAULT_CATALOG, dbName, tableName, false);
        long tableId = tableSchema.getId();
        long indexId = tableSchema.getIndexMetas().get(0).getIndexId();

        assertFalse(partitions.isEmpty());

        for (TablePartition partition : partitions) {
            List<TablePartition.Tablet> tablets = partition.getTablets();
            assertFalse(tablets.isEmpty());

            for (TablePartition.Tablet tablet : tablets) {
                Long tabletId = tablet.getId();
                try {
                    String storagePath = stageDir + "/" + tableId + "/" + partition.getId() + "/"
                            + indexId + "/" + tabletId;
                    Map<String, String> options = settings.toMap();
                    String metaUrl = tablet.getMetaUrls().get(0);
                    String metaContext = restClient.getTabletMeta(metaUrl);
                    options.put("starrocks.format.metaContext", metaContext);
                    StarRocksWriter writer = new StarRocksWriter(tabletId,
                            -1L,
                            tabletSchema,
                            storagePath,
                            options);
                    writer.open();
                    // write use arrow interface
                    try (VectorSchemaRoot vsr = VectorSchemaRoot.create(tabletSchema, writer.getRootAllocator())) {
                        fillSampleData(vsr, 0, 200);
                        writer.write(vsr);
                        vsr.clear();

                        fillSampleData(vsr, 200, 200);
                        writer.write(vsr);
                        vsr.clear();
                    }
                    writer.flush();
                    writer.finish();
                    writer.close();
                    writer.release();
                } catch (Exception e) {
                    e.printStackTrace();
                    if (assertOK) {
                        fail();
                    } else {
                        return;
                    }
                }
            }
        }

        String label = String.format("bypass_write_%s_%s_%s", dbName, tableName, uuid);
        String ak = settings.getS3AccessKey();
        String sk = settings.getS3SecretKey();
        String endpoint = settings.getS3Endpoint();

        loadSegmentData(dbName, label, stageDir, tableName, ak, sk, endpoint);
        boolean res = waitUtilLoadFinished(dbName, label);
        assertTrue(res);

        List<Map<String, String>> outputs = getTableRowNum(dbName, tableName);
        assertEquals(1, outputs.size());
        assertEquals(400, Integer.valueOf(outputs.get(0).get("num")));

        List<Map<String, String>> outputsNotNull = getTableRowNumNotNUll(dbName, tableName);
        assertEquals(1, outputsNotNull.size());
        assertEquals(399, Integer.valueOf(outputsNotNull.get(0).get("num")));
    }

    @ParameterizedTest
    @MethodSource("testPartitionTable")
    public void testPartitionTable(String tableName) throws LoadNonSupportException, RequestException {
        String uuid = RandomStringUtils.randomAlphabetic(8);
        String stageDir = "s3a://bucket1/.staging_ut/" + uuid + "/";
        TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, dbName, tableName);
        Validator.validateSegmentLoadExport(tableSchema);
        Schema tabletSchema = StarRocksUtils.toArrowSchema(tableSchema);

        List<TablePartition> partitions = restClient.listTablePartitions(DEFAULT_CATALOG, dbName, tableName, false);
        long tableId = tableSchema.getId();
        long indexId = tableSchema.getIndexMetas().get(0).getIndexId();

        assertFalse(partitions.isEmpty());

        int index = 0;
        for (TablePartition partition : partitions) {
            List<TablePartition.Tablet> tablets = partition.getTablets();
            assertFalse(tablets.isEmpty());

            for (TablePartition.Tablet tablet : tablets) {
                Long tabletId = tablet.getId();
                int startRow = index * 400;
                index++;
                try {
                    String storagePath = stageDir + "/" + tableId + "/" + partition.getId() + "/"
                            + indexId + "/" + tabletId;
                    Map<String, String> options = settings.toMap();
                    String metaUrl = tablet.getMetaUrls().get(0);
                    String metaContext = restClient.getTabletMeta(metaUrl);
                    options.put("starrocks.format.metaContext", metaContext);
                    StarRocksWriter writer = new StarRocksWriter(tabletId,
                            -1L,
                            tabletSchema,
                            storagePath,
                            options);
                    writer.open();
                    // write use arrow interface
                    try (VectorSchemaRoot vsr = VectorSchemaRoot.create(tabletSchema, writer.getRootAllocator())) {
                        fillSampleDataWithinDays(vsr, 0, 200);
                        writer.write(vsr);
                        vsr.clear();

                        fillSampleDataWithinDays(vsr, 200, 200);
                        writer.write(vsr);
                        vsr.clear();
                    }
                    writer.flush();
                    writer.finish();
                    writer.close();
                    writer.release();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail();
                }
            }
        }

        String label = String.format("bypass_write_%s_%s_%s", dbName, tableName, uuid);
        String ak = settings.getS3AccessKey();
        String sk = settings.getS3SecretKey();
        String endpoint = settings.getS3Endpoint();

        loadSegmentData(dbName, label, stageDir, tableName, ak, sk, endpoint);
        boolean res = waitUtilLoadFinished(dbName, label);
        assertTrue(res);

        List<Map<String, String>> outputs = getTableRowNumGroupByDay(dbName, tableName);
        assertEquals(2, outputs.size());
        assertEquals(1000, Integer.valueOf(outputs.get(0).get("num")));
        assertEquals("2024-08-25", outputs.get(0).get("c_date"));

        assertEquals(1000, Integer.valueOf(outputs.get(1).get("num")));
        assertEquals("2024-08-26", outputs.get(1).get("c_date"));
    }

    @ParameterizedTest
    @MethodSource("testListPartitionTable")
    public void testListPartitionTable(String tableName) throws LoadNonSupportException, RequestException {
        String uuid = RandomStringUtils.randomAlphabetic(8);
        String stageDir = "s3a://bucket1/.staging_ut/" + uuid + "/";
        TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, dbName, tableName);
        Validator.validateSegmentLoadExport(tableSchema);
        Schema tabletSchema = StarRocksUtils.toArrowSchema(tableSchema);

        List<TablePartition> partitions = restClient.listTablePartitions(DEFAULT_CATALOG, dbName, tableName, false);
        long tableId = tableSchema.getId();
        long indexId = tableSchema.getIndexMetas().get(0).getIndexId();

        assertFalse(partitions.isEmpty());

        int index = 0;
        for (TablePartition partition : partitions) {
            List<TablePartition.Tablet> tablets = partition.getTablets();
            assertFalse(tablets.isEmpty());

            for (TablePartition.Tablet tablet : tablets) {
                Long tabletId = tablet.getId();
                int startRow = index * 400;
                index++;
                try {
                    String storagePath = stageDir + "/" + tableId + "/" + partition.getId() + "/"
                            + indexId + "/" + tabletId;
                    Map<String, String> options = settings.toMap();
                    String metaUrl = tablet.getMetaUrls().get(0);
                    String metaContext = restClient.getTabletMeta(metaUrl);
                    options.put("starrocks.format.metaContext", metaContext);
                    StarRocksWriter writer = new StarRocksWriter(tabletId,
                            -1L,
                            tabletSchema,
                            storagePath,
                            options);
                    writer.open();
                    // write use arrow interface
                    try (VectorSchemaRoot vsr = VectorSchemaRoot.create(tabletSchema, writer.getRootAllocator())) {
                        fillSampleDataWithinDays(vsr, 0, 200);
                        writer.write(vsr);
                        vsr.clear();

                        fillSampleDataWithinDays(vsr, 200, 200);
                        writer.write(vsr);
                        vsr.clear();
                    }
                    writer.flush();
                    writer.finish();
                    writer.close();
                    writer.release();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail();
                }
            }
        }

        String label = String.format("bypass_write_%s_%s_%s", dbName, tableName, uuid);
        String ak = settings.getS3AccessKey();
        String sk = settings.getS3SecretKey();
        String endpoint = settings.getS3Endpoint();

        loadSegmentData(dbName, label, stageDir, tableName, ak, sk, endpoint);
        boolean res = waitUtilLoadFinished(dbName, label);
        assertTrue(res);

        List<Map<String, String>> outputs = getTableRowNumGroupByCity(dbName, tableName);
        assertEquals(4, outputs.size());
        assertEquals(200, Integer.valueOf(outputs.get(0).get("num")));
        assertEquals("Beijing", outputs.get(0).get("c_string"));

        assertEquals(200, Integer.valueOf(outputs.get(1).get("num")));
        assertEquals("Hangzhou", outputs.get(1).get("c_string"));

        assertEquals(200, Integer.valueOf(outputs.get(2).get("num")));
        assertEquals("Los Angeles", outputs.get(2).get("c_string"));

        assertEquals(200, Integer.valueOf(outputs.get(3).get("num")));
        assertEquals("San Francisco", outputs.get(3).get("c_string"));
    }

    // when rowId is 0, fill the max value,
    // 1 fill the min value,
    // 2 fill null,
    // >=4 fill the base value * rowId * sign.
    private static void fillSampleDataWithinDays(VectorSchemaRoot vsr, int startRowId, int numRows) {
        for (int colIdx = 0; colIdx < vsr.getSchema().getFields().size(); colIdx++) {
            Field field = vsr.getSchema().getFields().get(colIdx);
            FieldVector fieldVector = vsr.getVector(colIdx);
            for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
                int rowId = startRowId + rowIdx;
                if ("rowid".equalsIgnoreCase(field.getName())) {
                    ((IntVector) fieldVector).setSafe(rowIdx, rowId);
                    continue;
                }
                if ("rowid2".equalsIgnoreCase(field.getName())) {
                    ((IntVector) fieldVector).setSafe(rowIdx, rowId);
                    continue;
                }

                if (rowId == 2 && (!field.getFieldType().getType().toString().equalsIgnoreCase("Date(DAY)") && !"c_string".equalsIgnoreCase(field.getName()))) {
                    fieldVector.setNull(rowIdx);
                    if (fieldVector.getChildrenFromFields().size() > 0) {
                        for (FieldVector childVector : fieldVector.getChildrenFromFields()) {
                            childVector.setNull(rowIdx);
                        }
                    }
                    continue;
                }
                fillFieldWithinTwoDays(field, rowId, fieldVector, rowIdx, 0);
            }
            fieldVector.setValueCount(numRows);
        }

        vsr.setRowCount(numRows);
    }

    private static void fillFieldWithinTwoDays(Field field, int rowId, FieldVector fieldVector, int rowIdx, int depth) {
        String starRocksTypeName = field.getFieldType().getMetadata().get(StarRocksUtils.STARROKCS_COLUMN_TYPE);
        int sign = (rowId % 2 == 0) ? -1 : 1;
        DataType dataType = DataType.fromLiteral(starRocksTypeName).get();
        switch (dataType) {
            case BOOLEAN:
                ((BitVector) fieldVector).setSafe(rowIdx, 1- (rowId % 2));
                break;
            case TINYINT:
                if (rowId == 0) {
                    ((TinyIntVector) fieldVector).setSafe(rowIdx, Byte.MAX_VALUE);
                } else if (rowId == 1) {
                    ((TinyIntVector) fieldVector).setSafe(rowIdx, Byte.MIN_VALUE);
                } else {
                    ((TinyIntVector) fieldVector).setSafe(rowIdx, rowId * sign);
                }
                break;
            case SMALLINT:
                if (rowId == 0) {
                    ((SmallIntVector) fieldVector).setSafe(rowIdx, Short.MAX_VALUE);
                } else if (rowId == 1) {
                    ((SmallIntVector) fieldVector).setSafe(rowIdx, Short.MIN_VALUE);
                } else {
                    ((SmallIntVector) fieldVector).setSafe(rowIdx, (short) (rowId * 10 * sign));
                }
                break;
            case INT:
                if (rowId == 0) {
                    ((IntVector) fieldVector).setSafe(rowIdx, Integer.MAX_VALUE);
                } else if (rowId == 1) {
                    ((IntVector) fieldVector).setSafe(rowIdx, Integer.MIN_VALUE);
                } else {
                    ((IntVector) fieldVector).setSafe(rowIdx, rowId * 100 * sign + depth);
                }
                break;
            case BIGINT:
                if (rowId == 0) {
                    ((BigIntVector) fieldVector).setSafe(rowIdx, Long.MAX_VALUE);
                } else if (rowId == 1) {
                    ((BigIntVector) fieldVector).setSafe(rowIdx, Long.MIN_VALUE);
                } else {
                    ((BigIntVector) fieldVector).setSafe(rowIdx, rowId * 1000L * sign);
                }
                break;
            case LARGEINT:
                if (rowId == 0) {
                    ((Decimal256Vector) fieldVector).setSafe(rowIdx, new BigDecimal("99999999999999999999999999999999999999"));
                } else if (rowId == 1) {
                    ((Decimal256Vector) fieldVector).setSafe(rowIdx, new BigDecimal("-99999999999999999999999999999999999999"));
                } else {
                    ((Decimal256Vector) fieldVector).setSafe(rowIdx, BigDecimal.valueOf(rowId * 10000L * sign));
                }
                break;
            case FLOAT:
                ((Float4Vector) fieldVector).setSafe(rowIdx, 123.45678901234f * rowId * sign);
                break;
            case DOUBLE:
                ((Float8Vector) fieldVector).setSafe(rowIdx, 23456.78901234 * rowId * sign);
                break;
            case DECIMAL:
                // decimal v2 type
                BigDecimal bdv2;
                if (rowId == 0) {
                    bdv2 = new BigDecimal("-12345678901234567890123.4567");
                } else if (rowId == 1) {
                    bdv2 = new BigDecimal("999999999999999999999999.9999");
                } else {
                    bdv2 = new BigDecimal("1234.56789");
                    bdv2 = bdv2.multiply(BigDecimal.valueOf(sign));
                }
                ((DecimalVector) fieldVector).setSafe(rowIdx, bdv2);
                break;
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                ArrowType.Decimal decimalType = (ArrowType.Decimal) field.getFieldType().getType();
                BigDecimal bd;
                if (rowId == 0) {
                    if (decimalType.getPrecision() <= 9) {
                        bd = new BigDecimal("9999999.5678");
                    } else if (decimalType.getPrecision() <= 18) {
                        bd = new BigDecimal("999999999999999.56789");
                    } else {
                        bd = new BigDecimal("9999999999999999999999999999999999.56789");
                    }
                } else if (rowId == 1) {
                    if (decimalType.getPrecision() <= 9) {
                        bd = new BigDecimal("-9999999.5678");
                    } else if (decimalType.getPrecision() <= 18) {
                        bd = new BigDecimal("-999999999999999.56789");
                    } else {
                        bd = new BigDecimal("-9999999999999999999999999999999999.56789");
                    }
                } else {
                    if (decimalType.getPrecision() <= 9) {
                        bd = new BigDecimal("12345.5678");
                    } else if (decimalType.getPrecision() <= 18) {
                        bd = new BigDecimal("123456789012.56789");
                    } else {
                        bd = new BigDecimal("12345678901234567890123.56789");
                    }
                    bd = bd.multiply(BigDecimal.valueOf((long) rowId * sign));
                }
                bd = bd.setScale(decimalType.getScale(), RoundingMode.HALF_UP);
                ((DecimalVector) fieldVector).setSafe(rowIdx, bd);
                break;
            case CHAR:
            case VARCHAR: {
                String strValue = "";
                if (rowId % 4 == 0) {
                    strValue = "Beijing";
                } else if (rowId % 4 == 1) {
                    strValue = "Hangzhou";
                } else if (rowId % 4 == 2) {
                    strValue = "Los Angeles";
                } else if (rowId % 4 == 3) {
                    strValue = "San Francisco";
                }
                ((VarCharVector) fieldVector).setSafe(rowIdx, strValue.getBytes());
            }
            break;
            case OBJECT:
            case BITMAP: {
                byte[] bitmapValue = new byte[]{0x00};
                switch (rowId % 4) {
                    case 0:
                        bitmapValue = new byte[]{0x01, 0x00, 0x00, 0x00, 0x00};
                        break;
                    case 1:
                        bitmapValue = new byte[]{0x01, (byte) 0xE8, 0x03, 0x00, 0x00};
                        break;
                    case 3:
                        bitmapValue = new byte[]{0x1, (byte) 0xB8, 0xB, 0x0, 0x0};
                        break;
                }
                ((VarBinaryVector) fieldVector).setSafe(rowIdx, bitmapValue);
            }
            break;
            case HLL: {
                byte[] hllValue = new byte[]{0x00};
                switch (rowId % 4) {
                    case 0:
                        hllValue = new byte[]{0x00};
                        break;
                    case 1:
                        hllValue = new byte[]{0x1, 0x1, 0x44, 0x6, (byte) 0xC3, (byte) 0x80, (byte) 0x9E, (byte) 0x9D, (byte) 0xE6, 0x14};
                        break;
                    case 3:
                        hllValue = new byte[]{0x1, 0x1, (byte) 0x9A, 0x5, (byte) 0xE4, (byte) 0xE6, 0x65, 0x76, 0x4, 0x28};
                        break;
                }
                ((VarBinaryVector) fieldVector).setSafe(rowIdx, hllValue);
            }
            break;
            case BINARY:
            case VARBINARY:
                String valuePrefix = field.getName() + ":name" + rowId + ":";
                ByteBuffer buffer = ByteBuffer.allocate(valuePrefix.getBytes().length + 4);
                buffer.put(valuePrefix.getBytes());
                buffer.putInt(rowId);
                ((VarBinaryVector) fieldVector).setSafe(rowIdx, buffer.array());
                break;
            case JSON: {
                Gson gson = new Gson();
                Map<String, Object> jsonMap = new HashMap<>();
                jsonMap.put("rowid", rowId);
                boolean boolVal = rowId % 2 == 0;
                jsonMap.put("bool", boolVal);
                int intVal = 0;
                if (rowId == 0) {
                    intVal = Integer.MAX_VALUE;
                } else if (rowId == 1) {
                    intVal = Integer.MIN_VALUE;
                } else {
                    intVal = rowId * 100 * sign;
                }
                jsonMap.put("int", intVal);
                jsonMap.put("varchar", field.getName() + ":name" + rowId);
                String json = gson.toJson(jsonMap);
                ((VarCharVector) fieldVector).setSafe(rowIdx, json.getBytes(), 0, json.getBytes().length);
            }
            break;
            case DATE: {
                Date dt;
                if (sign == 1) {
                    dt = Date.valueOf("2024-08-25");
                } else {
                    dt = Date.valueOf("2024-08-26");
                }
                if (fieldVector instanceof DateDayVector) {
                    ((DateDayVector) fieldVector).setSafe(rowIdx, (int) dt.toLocalDate().toEpochDay());
                } else if (fieldVector instanceof DateMilliVector) {
                    ((DateMilliVector) fieldVector).setSafe(rowIdx, dt.toLocalDate().toEpochDay() * 24 * 3600 * 1000);
                } else {
                    throw new IllegalStateException("unsupported column type: " + field.getType());
                }
            }
            break;
            case DATETIME:
                LocalDateTime ts;
                if (rowId == 0) {
                    ts = LocalDateTime.parse("1800-11-20T12:34:56");
                } else if (rowId == 1) {
                    ts = LocalDateTime.parse("4096-11-30T11:22:33");
                } else {
                    ts = LocalDateTime.parse("2023-12-30T22:33:44");
                    ts = ts.withYear(1900 + 123 + rowId * sign);
                }
                ZoneOffset offset = ZoneId.systemDefault().getRules().getOffset(ts);
                ((TimeStampVector) fieldVector).setSafe(rowIdx, ts.toInstant(offset).toEpochMilli());
                break;
            case ARRAY: {
                List<FieldVector> children = fieldVector.getChildrenFromFields();
                int elementSize = (rowId + depth) % 4;
                ((ListVector) fieldVector).startNewValue(rowIdx);
                for (FieldVector childVector : children) {
                    if (childVector instanceof IntVector) {
                        int intVal = rowId * 100 * sign;
                        int startOffset = childVector.getValueCount();
                        for (int arrayIndex = 0; arrayIndex < elementSize; arrayIndex++) {
                            ((IntVector) childVector).setSafe(startOffset + arrayIndex, intVal + depth + arrayIndex);
                        }
                        childVector.setValueCount(startOffset + elementSize);
                    }
                }
                ((ListVector) fieldVector).endValue(rowIdx, elementSize);
            }
            break;
            case MAP: {
                List<FieldVector> children = fieldVector.getChildrenFromFields();
                int elementSize = rowId % 4;
                ((ListVector) fieldVector).startNewValue(rowIdx);
                UnionMapWriter mapWriter = ((MapVector) fieldVector).getWriter();
                mapWriter.setPosition(rowIdx);
                mapWriter.startMap();
                int intVal = rowId * 100 * sign;
                for (int arrayIndex = 0; arrayIndex < elementSize; arrayIndex++) {
                    mapWriter.startEntry();
                    mapWriter.key().integer().writeInt(intVal + depth + arrayIndex);
                    mapWriter.value().varChar().writeVarChar("mapvalue:" + (intVal + depth + arrayIndex));
                    mapWriter.endEntry();
                }
                mapWriter.endMap();
            }
            break;
            case STRUCT: {
                List<FieldVector> children = ((StructVector) fieldVector).getChildrenFromFields();
                for (FieldVector childVector : children) {
                    fillFieldWithinTwoDays(childVector.getField(), rowId, childVector, rowIdx, depth + 1);
                }
                ((StructVector) fieldVector).setIndexDefined(rowIdx);
            }
            break;
            default:
                throw new IllegalStateException("unsupported column type: " + field.getType());
        }
    }

    public List<Map<String, String>> getTableRowNum(String db, String table) {
        String queryStmt = String.format("select count(*) as num from `%s`.`%s`;", db, table);
        return executeSqlWithReturn(queryStmt, new ArrayList<>());
    }

    public List<Map<String, String>> getTableRowNumNotNUll(String db, String table) {
        String queryStmt = String.format("select count(*) as num from `%s`.`%s` where c_int is not null;", db, table);
        return executeSqlWithReturn(queryStmt, new ArrayList<>());
    }


    public List<Map<String, String>> getTableRowNumGroupByDay(String db, String table) {
        String queryStmt = String.format(" select c_date,count(*) AS num from  `%s`.`%s` " +
                "group by c_date order by c_date;", db, table);
        return executeSqlWithReturn(queryStmt, new ArrayList<>());
    }
    public List<Map<String, String>> getTableRowNumGroupByCity(String db, String table) {
        String queryStmt = String.format(" select c_string,count(*) AS num from  `%s`.`%s` " +
                "group by c_string order by c_string;", db, table);
        return executeSqlWithReturn(queryStmt, new ArrayList<>());
    }

    public void loadSegmentData(String db, String label, String stagingPath, String table,
                                String ak, String sk, String endpoint) {
        String loadSegment = String.format("LOAD LABEL %s.`%s` " +
                "( " +
                " DATA INFILE(\"%s\") " +
                " INTO TABLE %s " +
                " FORMAT AS \"starrocks\" " +
                ") WITH BROKER (" +
                "\"aws.s3.use_instance_profile\" = \"false\"," +
                "\"aws.s3.access_key\" = \"%s\"," +
                "\"aws.s3.secret_key\" = \"%s\"," +
                "\"aws.s3.endpoint\" = \"%s\"," +
                "\"aws.s3.enable_ssl\" = \"false\"" +
                ");", db, label, stagingPath, table, ak, sk, endpoint);
        executeSql(loadSegment);
    }

    public List<Map<String, String>> getSegmentLoadState(String db, String label) {
        String loadSegment = String.format("SHOW LOAD FROM %s WHERE LABEL = \"%s\" ORDER BY CreateTime desc limit 1;",
                db, label);
        return executeSqlWithReturn(loadSegment, new ArrayList<>());
    }

    public boolean waitUtilLoadFinished(String db, String label) {
        try {
            Thread.sleep(2000);
            String state;
            long timeout = 60000;
            long starTime = System.currentTimeMillis() / 1000;
            do {
                List<Map<String, String>> loads = getSegmentLoadState(db, label);
                if (loads.isEmpty()) {
                    return false;
                }
                // loads only have one row
                for (Map<String, String> l : loads) {
                    state = l.get("State");
                    if (state.equalsIgnoreCase("CANCELLED")) {
                        System.out.println("Load had failed with error: " + l.get("ErrorMsg"));
                        return false;
                    } else if (state.equalsIgnoreCase("Finished")) {
                        return true;
                    } else {
                        System.out.println("Load had not finished, try another loop with state = " + state);
                    }
                }
                Thread.sleep(2000);
            } while ((System.currentTimeMillis() / 1000 - starTime) < timeout);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return false;
    }
}
