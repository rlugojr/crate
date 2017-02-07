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

package io.crate.operation;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.google.common.collect.Iterables;
import io.crate.Streamer;
import io.crate.data.Bucket;
import io.crate.data.DataCursor;
import io.crate.data.Page;
import io.crate.data.Row;
import io.crate.jobs.PageBucketReceiver;
import io.crate.operation.merge.KeyIterable;
import io.crate.operation.merge.PagingIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class PageDownstreamDataCursor implements PageBucketReceiver, DataCursor {

    private final int numBuckets;
    private final PagingIterator<Integer, Row> pagingIterator;
    private final Executor executor;
    private final IntObjectHashMap<Bucket> pendingBuckets;

    private CompletableFuture<Page> firstPage = new CompletableFuture<>();
    private CompletableFuture<Page> nextPage = firstPage;

    public PageDownstreamDataCursor(Executor executor,
                                    int numBuckets,
                                    PagingIterator<Integer, Row> pagingIterator) {
        this.executor = executor;
        this.numBuckets = numBuckets;
        this.pagingIterator = pagingIterator;
        this.pendingBuckets = new IntObjectHashMap<>(numBuckets);
    }

    @Override
    public CompletableFuture<Page> getNext() {
        CompletableFuture<Page> nextPage = this.nextPage;
        if (nextPage.isDone() == false) {
            return Page.PENDING_FUTURE_ILLEGAL_STATE;
        }
        this.nextPage = new CompletableFuture<>();
        return nextPage;
    }

    @Override
    public void close() {
    }

    @Override
    public synchronized void setBucket(int bucketIdx, Bucket rows, boolean isLast, PageResultListener pageResultListener) {
        if (pendingBuckets.putIfAbsent(bucketIdx, rows)) {
            // TODO fail: bucket already set
        }
        if (pendingBuckets.size() == numBuckets) {
            List<KeyIterable<Integer, Row>> pageBuckets = new ArrayList<>(numBuckets);
            for (IntObjectCursor<Bucket> cursor : pendingBuckets) {
                pageBuckets.add(new KeyIterable<>(cursor.key, cursor.value));
            }
            pagingIterator.merge(pageBuckets);
            pendingBuckets.clear();

            nextPage.complete(new PagingIteratorPage(pagingIterator));
        }
    }

    @Override
    public void failure(int bucketIdx, Throwable throwable) {

    }

    @Override
    public void killed(int bucketIdx, Throwable throwable) {

    }

    @Override
    public Streamer<?>[] streamers() {
        return new Streamer<?>[0];
    }

    private static Iterable<? extends KeyIterable<Void,Row>> numberedBuckets(List<Bucket> buckets) {
        return Iterables.transform(buckets, b -> new KeyIterable<>(null, b));
    }

    private class PagingIteratorPage implements Page {
        public PagingIteratorPage(PagingIterator<Integer, Row> pagingIterator) {
        }

        @Override
        public CompletableFuture<Page> getNext() {
            return null;
        }

        @Override
        public Iterator<Row> data() {
            return null;
        }

        @Override
        public boolean isLast() {
            return false;
        }
    }
}
