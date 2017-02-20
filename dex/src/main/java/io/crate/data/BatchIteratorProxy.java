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

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class BatchIteratorProxy implements BatchIterator, BatchConsumer {

    private Runnable loadSourceTrigger = null;
    private CompletableFuture<BatchIterator> sourceFuture = null;
    private BatchIterator source = null;
    private Row row = OFF_ROW;

    public BatchIteratorProxy() {
    }

    public void setLoadSourceTrigger(Runnable trigger) {
        this.loadSourceTrigger = trigger;
    }

    @Override
    public void moveToStart() {
        row = OFF_ROW;
        if (source != null) {
            source.moveToStart();
        }
    }

    @Override
    public boolean moveNext() {
        if (source != null && source.moveNext()) {
            row = source.currentRow();
            return true;
        }
        row = OFF_ROW;
        return false;
    }

    @Override
    public Row currentRow() {
        return row;
    }

    @Override
    public void close() {
        if (source != null) {
            source.close();
        }
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        if (source == null) {
            return waitForSource();
        }
        return source.loadNextBatch();
    }

    private CompletionStage<?> waitForSource() {
        if (sourceFuture == null) {
            if (loadSourceTrigger != null) {
                loadSourceTrigger.run();
            }
            sourceFuture = new CompletableFuture<>();
            return sourceFuture;
        }
        return CompletableFutures.failedFuture(new IllegalStateException("BatchIterator is already loading"));
    }

    @Override
    public boolean allLoaded() {
        if (source == null) {
            return false;
        }
        return source.allLoaded();
    }

    @Override
    public void accept(BatchIterator iterator, @Nullable Throwable failure) {
        if (failure == null) {
            source = iterator;
        }
        if (sourceFuture != null) {
            if (failure == null) {
                sourceFuture.complete(iterator);
            } else {
                sourceFuture.completeExceptionally(failure);
            }
        }
    }
}
