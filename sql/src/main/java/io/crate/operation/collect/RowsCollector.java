/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operation.collect;

import io.crate.data.Row;
import io.crate.data.RowsBatchIterator;
import io.crate.operation.projectors.RowReceiver;

import java.util.Collections;

public final class RowsCollector {

    private RowsCollector() {
    }

    public static CrateCollector empty(RowReceiver rowDownstream) {
        return new BatchIteratorCollector(RowsBatchIterator.empty(), rowDownstream);
    }

    public static CrateCollector single(Row row, RowReceiver rowDownstream) {
        return new BatchIteratorCollector(RowsBatchIterator.newInstance(Collections.singletonList(row), row.numColumns()), rowDownstream);
    }

    public static CrateCollector forRows(Iterable<Row> rows, int numCols, RowReceiver rowReceiver) {
        return new BatchIteratorCollector(RowsBatchIterator.newInstance(rows, numCols), rowReceiver);
    }

    static CrateCollector.Builder emptyBuilder() {
        return RowsCollector::empty;
    }

    public static CrateCollector.Builder builder(final Iterable<Row> rows, int numCols) {
        return rowReceiver -> forRows(rows, numCols, rowReceiver);
    }
}
