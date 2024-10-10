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

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Map;

enum ColumnPruneType {
    REQUIRED,
    OUTPUT
}

public class StarRocksReader {
    static JniWrapper jniWrapper = JniWrapper.get();
    private long tabletId;
    private Schema requiredSchema;
    private Schema outputSchema;
    private String tabletRootPath;
    private Map<String, String> options;
    // nativeReader is the c++ StarrocksFormatReader potiner
    private long nativeReader = 0;


    BufferAllocator rootAllocator;
    private boolean released = false;

    public StarRocksReader(long tabletId, long version,
                           Schema requiredSchema,
                           Schema outputSchema,
                           String tabletRootPath, Map<String, String> options) {

        checkSchema(requiredSchema, ColumnPruneType.REQUIRED);
        checkSchema(outputSchema, ColumnPruneType.OUTPUT);
        this.tabletId = tabletId;
        this.requiredSchema = requiredSchema;
        this.outputSchema = outputSchema;
        this.tabletRootPath = tabletRootPath;
        this.options = options;
        rootAllocator = new RootAllocator();

        ArrowSchema requiredArrowSchema = ArrowSchema.allocateNew(rootAllocator);
        Data.exportSchema(rootAllocator, this.requiredSchema, null, requiredArrowSchema);

        ArrowSchema outputArrowSchema = ArrowSchema.allocateNew(rootAllocator);
        Data.exportSchema(rootAllocator, this.outputSchema, null, outputArrowSchema);

        nativeReader = createNativeReader(tabletId,
                version,
                requiredArrowSchema.memoryAddress(),
                outputArrowSchema.memoryAddress(),
                tabletRootPath,
                options);
    }

    public void open() {
        checkState();
        nativeOpen(nativeReader);
    }

    public void close() {
        checkState();
        nativeClose(nativeReader);
    }

    public VectorSchemaRoot getNext() {
        checkState();
        VectorSchemaRoot vsr = VectorSchemaRoot.create(outputSchema, rootAllocator);
        try (ArrowArray arrowArray = ArrowArray.allocateNew(rootAllocator)) {
            try {
                nativeGetNext(nativeReader, arrowArray.memoryAddress());
                Data.importIntoVectorSchemaRoot(rootAllocator, arrowArray, vsr, null);
            } catch (Exception e) {
                arrowArray.release();
                throw e;
            }
        }
        return vsr;
    }

    public void release() {
        JniWrapper.get().releaseReader(nativeReader);
        nativeReader = 0;
        released = true;
    }

    private static void checkSchema(Schema schema, ColumnPruneType columnPruneType) {
        if (ColumnPruneType.REQUIRED.equals(columnPruneType) && (schema == null || schema.getFields().isEmpty())) {
            throw new RuntimeException("Schema should not be empty!");
        }
    }

    private void checkState() {
        if (0 == nativeReader) {
            throw new IllegalStateException("Native reader may not be created correctly.");
        }

        if (released) {
            throw new IllegalStateException("Native reader is released.");
        }
    }

    public BufferAllocator getRootAllocator() {
        return rootAllocator;
    }

    /* native methods */

    public native long createNativeReader(long tabletId,
                                          long version,
                                          long requiredArrowSchemaAddr,
                                          long outputArrowSchemaAddr,
                                          String tableRootPath,
                                          Map<String, String> options);

    public native long nativeOpen(long nativeReader);

    public native long nativeClose(long nativeReader);

    public native long nativeGetNext(long nativeReader, long arrowArray);
}
