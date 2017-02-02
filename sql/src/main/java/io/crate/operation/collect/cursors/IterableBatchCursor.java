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

package io.crate.operation.collect.cursors;

import io.crate.core.collections.Row;
import io.crate.operation.data.BatchCursor;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

public class IterableBatchCursor implements BatchCursor {

    private final Iterable<Row> rows;
    private final int numCols;
    private Iterator<Row> it;
    private Row currentRow;
    private boolean closed = false;

    public IterableBatchCursor(Iterable<Row> rows, int numCols) {
        this.rows = rows;
        this.numCols = numCols;
        moveFirst();
    }

    @Override
    public int size() {
        return numCols;
    }

    @Override
    public Object get(int index) {
        return currentRow.get(index);
    }

    @Override
    public Object[] materialize() {
        return currentRow.materialize();
    }

    @Override
    public boolean moveFirst() {
        it = rows.iterator();
        return moveNext();
    }

    @Override
    public boolean moveNext() {
        if (it.hasNext()) {
            currentRow = it.next();
            return true;
        }
        currentRow = null;
        return false;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public Status status() {
        if (closed) {
            return Status.CLOSED;
        }
        if (currentRow == null) {
            return Status.OFF_ROW;
        }
        return Status.ON_ROW;
    }

    @Override
    public CompletableFuture<?> loadNextBatch() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean allLoaded() {
        return true;
    }
}
