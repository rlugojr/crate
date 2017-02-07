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

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.core.MultiFutureCallback;
import io.crate.data.*;
import io.crate.operation.merge.KeyIterable;
import io.crate.operation.merge.PagingIterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class PageDownstreamDataSource implements PageDownstream, DataSource {

    private final CompletableFuture<Page> firstPage = new CompletableFuture<>();
    private final Executor executor;
    private final PagingIterator<Void, Row> pagingIterator;

    private static class IteratorPage implements Page {

        private final PagingIterator<Void, Row> pagingIterator;
        private final PageConsumeListener listener;

        public IteratorPage(PagingIterator<Void, Row> pagingIterator, PageConsumeListener listener) {
            this.pagingIterator = pagingIterator;
            this.listener = listener;
        }

        @Override
        public CompletableFuture<Page> loadNext() {
            if (pagingIterator.hasNext()) {
                // fail;
            }
            listener.needMore();
            return null;
        }

        @Override
        public Iterator<Row> data() {
            return pagingIterator;
        }

        @Override
        public boolean isLast() {
            return false;
        }
    }

    public PageDownstreamDataSource(Executor executor, PagingIterator<Void, Row> pagingIterator) {
        this.executor = executor;
        this.pagingIterator = pagingIterator;
    }

    @Override
    public void nextPage(BucketPage page, PageConsumeListener listener) {
        FutureCallback<List<Bucket>> finalCallback = new FutureCallback<List<Bucket>>() {
            @Override
            public void onSuccess(@Nullable List<Bucket> result) {
                pagingIterator.merge(numberedBuckets(result));
                firstPage.complete(new IteratorPage(pagingIterator, listener));
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                firstPage.completeExceptionally(t);
            }
        };
        Executor executor = RejectionAwareExecutor.wrapExecutor(this.executor, finalCallback);
        MultiFutureCallback<Bucket> multiFutureCallback = new MultiFutureCallback<>(page.buckets().size(), finalCallback);
        for (ListenableFuture<Bucket> bucketFuture : page.buckets()) {
            Futures.addCallback(bucketFuture, multiFutureCallback, executor);
        }
    }

    private static Iterable<? extends KeyIterable<Void,Row>> numberedBuckets(List<Bucket> buckets) {
        return Iterables.transform(buckets, b -> new KeyIterable<>(null, b));
    }

    @Override
    public void finish() {
    }

    @Override
    public void fail(Throwable t) {
    }

    @Override
    public void kill(Throwable t) {
    }

    @Override
    public CompletableFuture<Page> loadFirst() {
        return firstPage;
    }

    @Override
    public void close() {
    }
}
