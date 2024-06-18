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

public class StarRocksWriter {

    static JniWrapper jniWrapper = JniWrapper.get();

    private final Long tabletId;
    private final Schema schema;
    private final Long txnId;
    private final String tabletRootPath;
    private final Map<String, String> options;

    // nativeWriter is the c++ StarRocksFormatWriter potiner
    private long nativeWriter = 0;
    BufferAllocator rootAllocator;
    private volatile boolean released = false;

    public StarRocksWriter(long tabletId,
                           long txnId,
                           Schema schema,
                           String tabletRootPath,
                           Map<String, String> options) {
        checkSchema(schema);
        this.tabletId = tabletId;
        this.schema = schema;
        this.txnId = txnId;
        this.tabletRootPath = tabletRootPath;
        this.options = options;

        rootAllocator = new RootAllocator();

        ArrowSchema arrowSchema = ArrowSchema.allocateNew(rootAllocator);
        Data.exportSchema(rootAllocator, this.schema, null, arrowSchema);
        this.nativeWriter = createNativeWriter(
                tabletId,
                txnId,
                arrowSchema.memoryAddress(),
                tabletRootPath,
                options);
    }

    public BufferAllocator getRootAllocator() {
        return rootAllocator;
    }

    public void open() {
        checkState();
        nativeOpen(nativeWriter);
    }

    public void close() {
        checkState();
        nativeClose(nativeWriter);
    }

    public long write(VectorSchemaRoot vsr) {
        checkState();
        ArrowArray arrowArray = ArrowArray.allocateNew(rootAllocator);
        Data.exportVectorSchemaRoot(rootAllocator, vsr, null, arrowArray);
        return nativeWrite(nativeWriter, arrowArray.memoryAddress());
    }

    public long flush() {
        checkState();
        return nativeFlush(nativeWriter);
    }


    public long finish() {
        checkState();
        return nativeFinish(nativeWriter);
    }

    public void release() {
        JniWrapper.get().releaseWriter(nativeWriter);
        nativeWriter = 0;
        released = true;
    }

    private static void checkSchema(Schema schema) {
        if (schema == null || schema.getFields().isEmpty()) {
            throw new RuntimeException("Schema should not be empty!");
        }
    }

    private void checkState() {
        if (0 == nativeWriter) {
            throw new IllegalStateException("Native writer may not be created correctly.");
        }

        if (released) {
            throw new IllegalStateException("Native writer is released.");
        }
    }

    /* native methods */

    public native long createNativeWriter(long tabletId,
                                          long txnId,
                                          long schema,
                                          String tableRootPath,
                                          Map<String, String> options);

    public native long nativeOpen(long nativeWriter);

    public native long nativeWrite(long nativeWriter, long arrowArray);

    public native long nativeFlush(long nativeWriter);

    public native long nativeFinish(long nativeWriter);

    public native long nativeClose(long nativeWriter);

}
