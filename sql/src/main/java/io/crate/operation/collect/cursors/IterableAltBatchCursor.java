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

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

public class IterableAltBatchCursor implements AltBatchCursor {

    private final Iterable<Row> rows;
    private final Action nextRowAction;

    private Iterator<Row> it;

    public IterableAltBatchCursor(Iterable<Row> rows) {
        this.rows = rows;
        it = rows.iterator();

        nextRowAction = new Action() {
                @Override
                public State state() {
                    return State.ON_ROW;
                }

                @Override
                public CompletableFuture<?> loadNext() {
                    CompletableFuture<Object> f = new CompletableFuture<>();
                    f.completeExceptionally(new IllegalStateException("..."));
                    return f;
                }

                @Override
                public Row getRow() {
                    return it.next();
                }
            };
    }

    @Override
    public Action nextAction() {
        if (it.hasNext()) {
            return nextRowAction;
        }
        return NO_MORE_DATA;
    }

    @Override
    public void moveFirst() {
        it = rows.iterator();
    }

    @Override
    public void close() {
    }
}
