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

import com.google.common.collect.Iterators;
import io.crate.data.Bucket;
import io.crate.data.DataSource;
import io.crate.data.Page;
import io.crate.data.Row;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

public class LimitingDataSource implements DataSource {

    private final DataSource source;
    private final int limit;

    public LimitingDataSource(DataSource source, int limit) {
        this.source = source;
        this.limit = limit;
    }

    @Override
    public CompletableFuture<Page> loadFirst() {
        return source.loadFirst().thenApply(this::applyLimit);
    }

    private Page applyLimit(Page page) {
        return new LimitedPage(page, limit);
    }

    @Override
    public void close() {
        source.close();
    }

    private class LimitedPage implements Page {
        private final Page page;
        private final int limit;

        LimitedPage(Page page, int limit) {
            this.page = page;
            this.limit = limit;
        }

        @Override
        public CompletableFuture<Page> loadNext() {
            // FIXME: would need to change the limit to the remaining limit
            return page.loadNext().thenApply(page -> new LimitedPage(page, limit));
        }

        @Override
        public Bucket bucket() {
            return new LimitedBucket(page.bucket(), limit);
        }

        @Override
        public boolean isLast() {
            return page.isLast();
        }
    }

    private class LimitedBucket implements Bucket {
        private final Bucket bucket;
        private final int limit;

        LimitedBucket(Bucket bucket, int limit) {
            this.bucket = bucket;
            this.limit = limit;
        }

        @Override
        public int size() {
            return limit;
        }

        @Override
        public Iterator<Row> iterator() {
            return Iterators.limit(bucket.iterator(), limit);
        }
    }
}
