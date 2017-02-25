/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.data;

import io.crate.concurrent.CompletableFutures;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * BatchIterator which gets its data from an input list and uses an iterable to move positions.
 */
public class IterableBackedBatchIterator implements BatchIterator {

    private static final List SINGLE_ROW = Collections.singletonList(null);

    private final InputList rowData;
    private final Iterable<?> iterable;
    private Iterator<?> iterator;

    /**
     * Returns an empty batch iterator with no columns
     */
    public static BatchIterator empty() {
        return newInstance(InputList.EMPTY, Collections.emptyList());
    }

    /**
     * Returns a batch iterator with a single row;
     */
    public static BatchIterator singleRow(InputList inputList) {
        return newInstance(inputList, SINGLE_ROW);
    }

    public static BatchIterator newInstance(InputList rowData, Iterable<?> iterable) {
        return new CloseAssertingBatchIterator(new IterableBackedBatchIterator(rowData, iterable));
    }

    private IterableBackedBatchIterator(InputList rowData, Iterable<?> iterable) {
        this.rowData = rowData;
        this.iterable = iterable;
        moveToStart();
    }

    @Override
    public InputList rowData() {
        return rowData;
    }

    @Override
    public void moveToStart() {
        iterator = iterable.iterator();
    }

    @Override
    public boolean moveNext() {
        if (iterator.hasNext()){
            iterator.next();
            return true;
        }
        return false;
    }

    @Override
    public void close() {
        iterator = null;
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        return CompletableFutures.failedFuture(new IllegalStateException("All batches already loaded"));
    }

    @Override
    public boolean allLoaded() {
        return true;
    }
}
