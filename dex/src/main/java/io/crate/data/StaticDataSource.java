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

import com.google.common.collect.Iterables;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;

public class StaticDataSource implements DataSource {

    public static StaticDataSource.Builder builder(Iterable<Row> rows) {
        return new Builder(rows);
    }

    public static class Builder implements DataSource.Builder {

        private final Iterable<Row> rows;

        private Builder(Iterable<Row> rows) {
            this.rows = rows;
        }

        @Override
        public DataSource.Builder skip(int offset) {
            return new Builder(Iterables.skip(rows, offset));
        }

        @Override
        public DataSource.Builder limit(int limit) {
            return new Builder(Iterables.limit(rows, limit));
        }

        @Override
        public DataSource.Builder filter(Predicate<Row> filter) {
            return new Builder(Iterables.filter(rows, filter::test));
        }

        @Override
        public DataSource.Builder addTransformation(Function<Iterable<Row>, Iterable<Row>> transformation) {
            return new Builder(transformation.apply(rows));
        }

        @Override
        public DataSource build() {
            return new StaticDataSource(rows);
        }
    }

    private final CompletableFuture<Page> first;

    private StaticDataSource(Iterable<Row> rows) {
        first = CompletableFuture.completedFuture(new Page() {
            @Override
            public CompletableFuture<Page> loadNext() {
                CompletableFuture<Page> future = new CompletableFuture<>();
                future.completeExceptionally(new IllegalStateException("Cannot call loadNext on last page"));
                return future;
            }

            @Override
            public Iterator<Row> data() {
                return rows.iterator();
            }

            @Override
            public boolean isLast() {
                return true;
            }
        });
    }

    @Override
    public CompletableFuture<Page> loadFirst() {
        return first;
    }

    @Override
    public void close() {
    }
}
