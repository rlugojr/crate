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

package io.crate.operation.collect.collectors;

import io.crate.concurrent.CompletableFutures;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.operation.merge.*;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * Factory to create a BatchIterator which is backed by 1 or more {@link OrderedDocCollector}.
 * This BatchIterator exposes data stored in a Lucene index and utilizes Lucene sort for efficient sorting.
 */
public class OrderedLuceneBatchIteratorFactory {

    public static BatchIterator newInstance(List<OrderedDocCollector> orderedDocCollectors,
                                            Comparator<Row> rowComparator,
                                            Executor executor,
                                            boolean requiresScroll) {
        return new Factory(
            orderedDocCollectors, rowComparator, executor, requiresScroll).create();
    }

    private static class Factory {

        private final List<OrderedDocCollector> orderedDocCollectors;
        private final Executor executor;
        private final PagingIterator<ShardId, Row> pagingIterator;
        private final Function<ShardId, Boolean> tryFetchMore;
        private final Map<ShardId, OrderedDocCollector> collectorsByShardId;

        private BatchPagingIterator<ShardId> batchPagingIterator;

        Factory(List<OrderedDocCollector> orderedDocCollectors,
                Comparator<Row> rowComparator,
                Executor executor,
                boolean requiresScroll) {
            this.orderedDocCollectors = orderedDocCollectors;
            this.executor = executor;
            if (orderedDocCollectors.size() == 1) {
                OrderedDocCollector orderedDocCollector = orderedDocCollectors.get(0);
                tryFetchMore = shardId -> loadFrom(orderedDocCollector);
                pagingIterator = requiresScroll ?
                    PassThroughPagingIterator.repeatable() : PassThroughPagingIterator.oneShot();
                collectorsByShardId = null;
            } else {
                collectorsByShardId = toMapByShardId(orderedDocCollectors);
                tryFetchMore = this::manyTryFetchMore;
                pagingIterator = new SortedPagingIterator<>(rowComparator, requiresScroll);
            }
        }

        BatchIterator create() {
            batchPagingIterator = new BatchPagingIterator<>(
                pagingIterator,
                tryFetchMore,
                this::areAllExhausted,
                this::close
            );
            return batchPagingIterator;
        }

        private Boolean manyTryFetchMore(ShardId shardId) {
            if (areAllExhausted()) {
                return false;
            }
            if (shardId == null) {
                loadFromAllUnExhausted(orderedDocCollectors, executor).whenComplete(this::onNextRows);
                return true;
            } else {
                return loadFrom(collectorsByShardId.get(shardId));
            }
        }

        private boolean loadFrom(OrderedDocCollector orderedDocCollector) {
            KeyIterable<ShardId, Row> rows;
            try {
                rows = orderedDocCollector.get();
            } catch (Exception e) {
                batchPagingIterator.completeLoad(e);
                return false;
            }
            pagingIterator.merge(Collections.singletonList(rows));
            if (orderedDocCollector.exhausted) {
                pagingIterator.finish();
            }
            batchPagingIterator.completeLoad(null);
            return true;
        }

        private void onNextRows(List<KeyIterable<ShardId, Row>> rowsList, @Nullable Throwable throwable) {
            if (throwable == null) {
                pagingIterator.merge(rowsList);
                if (areAllExhausted()) {
                    pagingIterator.finish();
                }
            }
            batchPagingIterator.completeLoad(throwable);
        }

        private void close() {
            for (OrderedDocCollector orderedDocCollector : orderedDocCollectors) {
                orderedDocCollector.close();
            }
        }

        private boolean areAllExhausted() {
            for (OrderedDocCollector orderedDocCollector : orderedDocCollectors) {
                if (!orderedDocCollector.exhausted) {
                    return false;
                }
            }
            return true;
        }
    }

    private static CompletableFuture<List<KeyIterable<ShardId, Row>>> loadFromAllUnExhausted(List<OrderedDocCollector> orderedDocCollectors,
                                                                                             Executor executor) {
        List<CompletableFuture<KeyIterable<ShardId, Row>>> futures = new ArrayList<>(orderedDocCollectors.size());
        for (OrderedDocCollector orderedDocCollector : orderedDocCollectors.subList(1, orderedDocCollectors.size())) {
            if (orderedDocCollector.exhausted) {
                continue;
            }
            futures.add(CompletableFuture.supplyAsync(orderedDocCollector, executor));
        }
        futures.add(CompletableFuture.completedFuture(orderedDocCollectors.get(0).get()));
        return CompletableFutures.allAsList(futures);
    }

    private static Map<ShardId, OrderedDocCollector> toMapByShardId(List<OrderedDocCollector> orderedDocCollectors) {
        Map<ShardId, OrderedDocCollector> collectorsByShardId = new HashMap<>(orderedDocCollectors.size());
        for (OrderedDocCollector orderedDocCollector : orderedDocCollectors) {
            collectorsByShardId.put(orderedDocCollector.shardId(), orderedDocCollector);
        }
        return collectorsByShardId;
    }
}
