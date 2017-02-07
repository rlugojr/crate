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

package io.crate.data.transform;

import io.crate.data.CollectionBucket;
import io.crate.data.DataCursor;
import io.crate.data.Page;
import io.crate.data.Row;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class TopNOrderByCursor implements DataCursor {

    private final DataCursor source;
    private final int limit;

    private final CompletableFuture<Page> result = new CompletableFuture<>();
    private final PriorityQueue<Object[]> q;

    public TopNOrderByCursor(DataCursor source, int limit, Comparator<Object[]> rowComparator) {
        this.source = source;
        this.limit = limit;
        this.q = new PriorityQueue<>(limit, rowComparator);
    }

    @Override
    public CompletableFuture<Page> getNext() {
        source.getNext().whenComplete(this::receivePage);
        return result;
    }

    private void receivePage(Page page, Throwable t) {
        if (t == null) {
            consumePage(page);
        } else {
            result.completeExceptionally(t);
        }
    }

    private void consumePage(Page page) {
        Iterator<Row> it = page.data();
        while (it.hasNext()) {
            Row row = it.next();
            q.offer(row.materialize());
        }
        if (page.isLast()) {
            setResult();
        } else {
            page.getNext().whenComplete(this::receivePage);
        }
    }

    private void setResult() {
        result.complete(new Page() {
            @Override
            public CompletableFuture<Page> getNext() {
                return null;
            }

            @Override
            public Iterator<Row> data() {
                List<Object[]> result = new ArrayList<>(limit);
                for (int i = 0; i < limit; i++) {
                    result.add(q.poll());
                }
                return new CollectionBucket(result).iterator();
            }

            @Override
            public boolean isLast() {
                return true;
            }
        });
    }

    @Override
    public void close() {
        source.close();
    }
}
