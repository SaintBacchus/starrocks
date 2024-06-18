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

import com.starrocks.format.rest.TransactionResult;
import com.starrocks.format.rest.model.TablePartition;
import com.starrocks.format.rest.model.TabletCommitInfo;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class StarRocksReaderWriterTest extends BaseFormatTest {

    @BeforeAll
    public static void init() throws Exception {
        BaseFormatTest.init();
    }

    private static Stream<Arguments> testReadAfterWrite() {
        return Stream.of(
                Arguments.of("tb_json_two_key_primary"),
                Arguments.of("tb_json_two_key_unique"),
                Arguments.of("tb_binary_two_key_duplicate"),
                Arguments.of("tb_binary_two_key_primary"),
                Arguments.of("tb_binary_two_key_unique")
        );
    }

    @ParameterizedTest
    @MethodSource("testReadAfterWrite")
    public void testReadAfterWrite(String tableName) throws Exception {
        String label = String.format("bypass_write_%s_%s_%s",
                DB_NAME, tableName, RandomStringUtils.randomAlphabetic(8));
        Schema schema = StarRocksUtils.toArrowSchema(restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName));
        Schema tableSchema = StarRocksUtils.toArrowSchema(restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName));
        List<TablePartition> partitions = restClient.listTablePartitions(DEFAULT_CATALOG, DB_NAME, tableName, false);
        assertFalse(partitions.isEmpty());

        // begin transaction
        TransactionResult beginTxnResult = restClient.beginTransaction(DEFAULT_CATALOG, DB_NAME, tableName, label);
        assertTrue(beginTxnResult.isOk());

        List<TabletCommitInfo> committedTablets = new ArrayList<>();
        for (TablePartition partition : partitions) {
            List<TablePartition.Tablet> tablets = partition.getTablets();
            assertFalse(tablets.isEmpty());

            int tabletIndex = 0;
            for (TablePartition.Tablet tablet : tablets) {
                Long tabletId = tablet.getId();
                Long backendId = tablet.getPrimaryComputeNodeId();
                int startId = tabletIndex * 1000;
                tabletIndex++;

                try {
                    StarRocksWriter writer = new StarRocksWriter(tabletId,
                            beginTxnResult.getTxnId(),
                            schema,
                            partition.getStoragePath(),
                            settings.toMap());
                    writer.open();
                    // write use chunk interface
                    VectorSchemaRoot vsr = VectorSchemaRoot.create(tableSchema, writer.getRootAllocator());

                    fillSampleData(vsr, startId, 4);
                    writer.write(vsr);
                    vsr.close();

                    fillSampleData(vsr, startId + 200, 4);
                    writer.write(vsr);
                    vsr.close();

                    writer.flush();
                    writer.finish();
                    writer.close();
                    writer.release();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail();
                }

                committedTablets.add(new TabletCommitInfo(tabletId, backendId));
            }
        }

        TransactionResult prepareTxnResult = restClient.prepareTransaction(
                DEFAULT_CATALOG, DB_NAME, beginTxnResult.getLabel(), committedTablets, null);
        assertTrue(prepareTxnResult.isOk());

        TransactionResult commitTxnResult = restClient.commitTransaction(
                DEFAULT_CATALOG, DB_NAME, prepareTxnResult.getLabel());
        assertTrue(commitTxnResult.isOk());

        // read all data test
        int expectedNumRows = 24;
        // read chunk
        long totalRows = 0;
        partitions = restClient.listTablePartitions(DEFAULT_CATALOG, DB_NAME, tableName, false);
        for (TablePartition partition : partitions) {
            long version = partition.getVisibleVersion();
            for (TablePartition.Tablet tablet : partition.getTablets()) {
                Long tabletId = tablet.getId();

                StarRocksReader reader = new StarRocksReader(
                        tabletId, version, tableSchema, tableSchema, partition.getStoragePath(), settings.toMap());
                reader.open();

                long numRows;
                do {
                    VectorSchemaRoot vsr = reader.getNext();
                    numRows = vsr.getRowCount();

                    checkValue(vsr, numRows);
                    vsr.close();

                    totalRows += numRows;
                } while (numRows > 0);

                // should be empty chunk
                VectorSchemaRoot vsr = reader.getNext();
                assertEquals(0, vsr.getRowCount());
                vsr.close();

                reader.close();
                reader.release();
            }
        }

        assertEquals(expectedNumRows, totalRows);

    }

    private static Stream<Arguments> testReadAfterWriteWithJsonFilter() {
        return Stream.of(
                Arguments.of("tb_json_two_key_primary"),
                Arguments.of("tb_json_two_key_unique")
        );
    }

    @ParameterizedTest
    @MethodSource("testReadAfterWriteWithJsonFilter")
    public void testReadAfterWriteWithJsonFilter(String tableName) throws Exception {
        String label = String.format("bypass_write_%s_%s_%s",
                DB_NAME, tableName, RandomStringUtils.randomAlphabetic(8));
        Schema tableSchema = StarRocksUtils.toArrowSchema(restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName));
        List<TablePartition> partitions = restClient.listTablePartitions(DEFAULT_CATALOG, DB_NAME, tableName, false);
        assertFalse(partitions.isEmpty());

        // begin transaction
        TransactionResult beginTxnResult = restClient.beginTransaction(DEFAULT_CATALOG, DB_NAME, tableName, label);
        assertTrue(beginTxnResult.isOk());

        List<TabletCommitInfo> committedTablets = new ArrayList<>();
        for (TablePartition partition : partitions) {
            List<TablePartition.Tablet> tablets = partition.getTablets();
            assertFalse(tablets.isEmpty());

            int tabletIndex = 0;
            for (TablePartition.Tablet tablet : tablets) {
                Long tabletId = tablet.getId();
                Long backendId = tablet.getPrimaryComputeNodeId();
                int startId = tabletIndex * 1000;
                tabletIndex++;

                try {
                    StarRocksWriter writer = new StarRocksWriter(tabletId,
                            beginTxnResult.getTxnId(),
                            tableSchema,
                            partition.getStoragePath(),
                            settings.toMap());
                    writer.open();
                    // write use chunk interface
                    VectorSchemaRoot vsr = VectorSchemaRoot.create(tableSchema, writer.getRootAllocator());

                    fillSampleData(vsr, startId, 4);
                    writer.write(vsr);
                    vsr.close();

                    fillSampleData(vsr, startId + 200, 4);
                    writer.write(vsr);
                    vsr.close();

                    writer.flush();
                    writer.finish();
                    writer.close();
                    writer.release();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail();
                }

                committedTablets.add(new TabletCommitInfo(tabletId, backendId));
            }
        }

        TransactionResult prepareTxnResult = restClient.prepareTransaction(
                DEFAULT_CATALOG, DB_NAME, beginTxnResult.getLabel(), committedTablets, null);
        assertTrue(prepareTxnResult.isOk());

        TransactionResult commitTxnResult = restClient.commitTransaction(
                DEFAULT_CATALOG, DB_NAME, prepareTxnResult.getLabel());
        assertTrue(commitTxnResult.isOk());

        // read all data test
        int expectedNumRows = 24;
        // read chunk
        long totalRows = 0;
        partitions = restClient.listTablePartitions(DEFAULT_CATALOG, DB_NAME, tableName, false);
        for (TablePartition partition : partitions) {
            long version = partition.getVisibleVersion();
            for (TablePartition.Tablet tablet : partition.getTablets()) {
                Long tabletId = tablet.getId();

                StarRocksReader reader = new StarRocksReader(
                        tabletId, version, tableSchema, tableSchema, partition.getStoragePath(), settings.toMap());
                reader.open();

                long numRows;
                do {
                    VectorSchemaRoot vsr = reader.getNext();
                    numRows = vsr.getRowCount();

                    checkValue(vsr, numRows);
                    vsr.close();

                    totalRows += numRows;
                } while (numRows > 0);

                // should be empty chunk
                VectorSchemaRoot vsr = reader.getNext();
                assertEquals(0, vsr.getRowCount());
                vsr.close();

                reader.close();
                reader.release();
            }
        }

        assertEquals(expectedNumRows, totalRows);

        // test with filter
        String requiredColumns = "rowid,c_varchar,c_json";
        String outputColumns = "rowid,c_varchar";
        String sql = "select rowId, c_varchar from demo." + tableName + " where cast((c_json->'rowid') as int) % 2 = 0";
        // get query plan
        String queryPlan = restClient.getQueryPlan(DB_NAME, tableName, sql).getOpaquedQueryPlan();
        Set<String> requiredColumnName = new HashSet<>(Arrays.asList(requiredColumns.split(",")));
        Set<String> outputColumnName = new HashSet<>(Arrays.asList(outputColumns.split(",")));

        // resolve required schema
        List<Field> fields = tableSchema.getFields().stream()
                .filter(col -> requiredColumnName.contains(col.getName().toLowerCase()))
                .collect(Collectors.toList());
        Schema requiredSchema = new Schema(fields, tableSchema.getCustomMetadata());
        // resolve output schema
        fields = tableSchema.getFields().stream()
                .filter(col -> outputColumnName.contains(col.getName().toLowerCase()))
                .collect(Collectors.toList());
        Schema outputSchema = new Schema(fields, tableSchema.getCustomMetadata());

        // read chunk
        int expectedTotalRows = 11;
        totalRows = 0;
        partitions = restClient.listTablePartitions(DEFAULT_CATALOG, DB_NAME, tableName, false);
        for (TablePartition partition : partitions) {
            long version = partition.getVisibleVersion();
            for (TablePartition.Tablet tablet : partition.getTablets()) {
                Long tabletId = tablet.getId();

                try {
                    Map<String, String> options = settings.toMap();
                    options.put(STARROCKS_FORMAT_QUERY_PLAN, queryPlan);
                    // read table
                    StarRocksReader reader = new StarRocksReader(
                            tabletId, version, requiredSchema, outputSchema, partition.getStoragePath(), options);
                    reader.open();

                    long numRows;
                    do {
                        VectorSchemaRoot vsr = reader.getNext();
                        numRows = vsr.getRowCount();

                        checkValue(vsr, numRows);
                        vsr.close();

                        totalRows += numRows;
                    } while (numRows > 0);

                    // should be empty chunk
                    VectorSchemaRoot vsr = reader.getNext();
                    assertEquals(0, vsr.getRowCount());
                    vsr.close();

                    reader.close();
                    reader.release();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail();
                }

            }
        }

        assertEquals(expectedTotalRows, totalRows);

    }

    private static Stream<Arguments> testComplexTypeReadAfterWrite() {
        return Stream.of(
                Arguments.of("tb_map_array_struct")
        );
    }
    @ParameterizedTest
    @MethodSource("testComplexTypeReadAfterWrite")
    public void testComplexTypeReadAfterWrite(String tableName) throws Exception {
        String label = String.format("bypass_write_%s_%s_%s",
                DB_NAME, tableName, RandomStringUtils.randomAlphabetic(8));
        Schema schema = StarRocksUtils.toArrowSchema(restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName));
        Schema tableSchema = StarRocksUtils.toArrowSchema(restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName));
        List<TablePartition> partitions = restClient.listTablePartitions(DEFAULT_CATALOG, DB_NAME, tableName, false);
        assertFalse(partitions.isEmpty());

        // begin transaction
        TransactionResult beginTxnResult = restClient.beginTransaction(DEFAULT_CATALOG, DB_NAME, tableName, label);
        assertTrue(beginTxnResult.isOk());

        List<TabletCommitInfo> committedTablets = new ArrayList<>();
        for (TablePartition partition : partitions) {
            List<TablePartition.Tablet> tablets = partition.getTablets();
            assertFalse(tablets.isEmpty());

            int tabletIndex = 0;
            for (TablePartition.Tablet tablet : tablets) {
                Long tabletId = tablet.getId();
                Long backendId = tablet.getPrimaryComputeNodeId();
                int startId = tabletIndex * 1000;
                tabletIndex++;

                try {
                    StarRocksWriter writer = new StarRocksWriter(tabletId,
                            beginTxnResult.getTxnId(),
                            tableSchema,
                            partition.getStoragePath(),
                            settings.toMap());
                    writer.open();
                    // write use chunk interface
                    try(VectorSchemaRoot vsr = VectorSchemaRoot.create(tableSchema, writer.getRootAllocator())) {

                        fillSampleData(vsr, startId, 4);
                        writer.write(vsr);
                        vsr.clear();

                        fillSampleData(vsr, startId + 200, 4);
                        writer.write(vsr);
                    }

                    writer.flush();
                    writer.finish();
                    writer.close();
                    writer.release();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail();
                }

                committedTablets.add(new TabletCommitInfo(tabletId, backendId));
            }
        }

        TransactionResult prepareTxnResult = restClient.prepareTransaction(
                DEFAULT_CATALOG, DB_NAME, beginTxnResult.getLabel(), committedTablets, null);
        assertTrue(prepareTxnResult.isOk());

        TransactionResult commitTxnResult = restClient.commitTransaction(
                DEFAULT_CATALOG, DB_NAME, prepareTxnResult.getLabel());
        assertTrue(commitTxnResult.isOk());

        // read all data test
        int expectedNumRows = 8;
        // read chunk
        long totalRows = 0;
        partitions = restClient.listTablePartitions(DEFAULT_CATALOG, DB_NAME, tableName, false);
        for (TablePartition partition : partitions) {
            long version = partition.getVisibleVersion();
            for (TablePartition.Tablet tablet : partition.getTablets()) {
                Long tabletId = tablet.getId();

                StarRocksReader reader = new StarRocksReader(
                        tabletId, version, tableSchema, tableSchema, partition.getStoragePath(), settings.toMap());
                reader.open();

                long numRows;
                do {
                    VectorSchemaRoot vsr = reader.getNext();
                    numRows = vsr.getRowCount();

                    checkValue(vsr, numRows);
                    vsr.close();

                    totalRows += numRows;
                } while (numRows > 0);

                // should be empty chunk
                VectorSchemaRoot vsr = reader.getNext();
                assertEquals(0, vsr.getRowCount());
                vsr.close();

                reader.close();
                reader.release();
            }
        }
        assertEquals(expectedNumRows, totalRows);
    }

}
