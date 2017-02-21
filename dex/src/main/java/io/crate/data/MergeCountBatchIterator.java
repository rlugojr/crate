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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class MergeCountBatchIterator implements BatchIterator {

    private final BatchIterator source;

    private CompletableFuture<?> loading = null;
    private Row row = OFF_ROW;
    private long sum = 0;
    private boolean firstMoveNext = true;

    public static BatchIterator newInstance(BatchIterator source) {
        return new CloseAssertingBatchIterator(new MergeCountBatchIterator(source));
    }

    private MergeCountBatchIterator(BatchIterator source) {
        this.source = source;
    }

    @Override
    public void moveToStart() {
        raiseIfLoading();
        source.moveToStart();
        row = OFF_ROW;
        loading = null;
        firstMoveNext = true;
        sum = 0;
    }

    @Override
    public boolean moveNext() {
        raiseIfLoading();
        if (firstMoveNext && loading != null) {
            row = new Row1(sum);
            firstMoveNext = false;
            return true;
        }
        row = OFF_ROW;
        return false;
    }

    @Override
    public Row currentRow() {
        raiseIfLoading();
        return row;
    }

    @Override
    public void close() {
        source.close();
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        if (loading == null) {
            loading = BatchRowVisitor.visitRows(source, this::onRow);
            return loading;
        }
        return CompletableFutures.failedFuture(new IllegalStateException("BatchIterator is already loading"));
    }

    @Override
    public boolean allLoaded() {
        if (loading == null) {
            return false;
        }
        if (loading.isDone() == false) {
            throw new IllegalStateException("BatchIterator is loading");
        }
        return true;
    }

    private void onRow(Row row) {
        sum += ((long) row.get(0));
    }

    private void raiseIfLoading() {
        if (loading != null && loading.isDone() == false) {
            throw new IllegalStateException("BatchIterator is loading");
        }
    }
}