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

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class OrderedLuceneBatchIterator {

    public static BatchIterator newInstance(List<OrderedDocCollector> orderedDocCollectors,
                                            Comparator<Row> rowComparator,
                                            Executor executor,
                                            boolean requiresScroll) {

        boolean singleShard = orderedDocCollectors.size() == 1;
        if (singleShard) {
            return singleShardBatchIterator(orderedDocCollectors.get(0), requiresScroll);
        }
        return manyShardsBatchIterator(orderedDocCollectors, rowComparator, requiresScroll, executor);
    }

    private static BatchIterator manyShardsBatchIterator(List<OrderedDocCollector> orderedDocCollectors,
                                                         Comparator<Row> rowComparator,
                                                         boolean requiresScroll,
                                                         Executor executor) {
        PagingIterator<ShardId, Row> pagingIterator = new SortedPagingIterator<>(rowComparator, requiresScroll);
        AtomicReference<BatchPagingIterator<ShardId>> batchItRef = new AtomicReference<>();
        Function<ShardId, Boolean> tryFetchMore = getTryFetchMoreFunction(
            orderedDocCollectors,
            batchItRef,
            pagingIterator,
            executor
        );
        BatchPagingIterator<ShardId> batchIt = new BatchPagingIterator<>(
            pagingIterator,
            tryFetchMore,
            () -> areAllExhausted(orderedDocCollectors),
            () -> {
                for (OrderedDocCollector orderedDocCollector : orderedDocCollectors) {
                    orderedDocCollector.close();
                }
            }
        );
        batchItRef.set(batchIt);
        return batchIt;
    }

    private static Function<ShardId, Boolean> getTryFetchMoreFunction(List<OrderedDocCollector> orderedDocCollectors,
                                                                      AtomicReference<BatchPagingIterator<ShardId>> batchItRef,
                                                                      PagingIterator<ShardId, Row> pagingIterator,
                                                                      Executor executor) {
        Map<ShardId, OrderedDocCollector> collectorsByShardId = toMapByShardId(orderedDocCollectors);
        return shardId -> {
            if (areAllExhausted(orderedDocCollectors)) {
                return false;
            }
            BatchPagingIterator<ShardId> batchPagingIterator = batchItRef.get();
            if (shardId == null) {
                loadFromAllUnExhausted(orderedDocCollectors, executor).whenComplete((r, f) -> {
                    if (f == null) {
                        pagingIterator.merge(r);
                        if (areAllExhausted(orderedDocCollectors)) {
                            pagingIterator.finish();
                        }
                        batchPagingIterator.completeLoad(null);
                    } else {
                        batchPagingIterator.completeLoad(f);
                    }
                });
            } else {
                OrderedDocCollector collector = collectorsByShardId.get(shardId);
                KeyIterable<ShardId, Row> rows;
                try {
                    rows = collector.get();
                } catch (Throwable t) {
                    batchPagingIterator.completeLoad(t);
                    return false;
                }
                pagingIterator.merge(Collections.singletonList(rows));
                if (areAllExhausted(orderedDocCollectors)) {
                    pagingIterator.finish();
                }
                batchPagingIterator.completeLoad(null);
            }
            return true;
        };
    }

    private static BatchIterator singleShardBatchIterator(OrderedDocCollector orderedDocCollector, boolean requiresScroll) {
        PagingIterator<ShardId, Row> pagingIterator =
            requiresScroll ? PassThroughPagingIterator.repeatable() : PassThroughPagingIterator.oneShot();

        AtomicReference<BatchPagingIterator<ShardId>> batchItRef = new AtomicReference<>();
        BatchPagingIterator<ShardId> batchIt = new BatchPagingIterator<>(
            pagingIterator,
            getTryFetchMore(orderedDocCollector, pagingIterator, batchItRef),
            orderedDocCollector::exhausted,
            orderedDocCollector::close
        );
        batchItRef.set(batchIt);
        return batchIt;
    }

    private static Function<ShardId, Boolean> getTryFetchMore(OrderedDocCollector orderedDocCollector,
                                                              PagingIterator<ShardId, Row> pagingIterator,
                                                              AtomicReference<BatchPagingIterator<ShardId>> batchItRef) {
        return shardId -> {
            if (orderedDocCollector.exhausted) {
                return false;
            }
            BatchPagingIterator<ShardId> batchIt = batchItRef.get();
            KeyIterable<ShardId, Row> rows;
            try {
                rows = orderedDocCollector.get();
            } catch (Exception e) {
                batchIt.completeLoad(e);
                return false;
            }
            pagingIterator.merge(Collections.singletonList(rows));
            if (orderedDocCollector.exhausted) {
                pagingIterator.finish();
            }
            batchIt.completeLoad(null);
            return true;
        };
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

    private static boolean areAllExhausted(List<OrderedDocCollector> orderedDocCollectors) {
        for (OrderedDocCollector orderedDocCollector : orderedDocCollectors) {
            if (!orderedDocCollector.exhausted) {
                return false;
            }
        }
        return true;
    }

    private static Map<ShardId, OrderedDocCollector> toMapByShardId(List<OrderedDocCollector> orderedDocCollectors) {
        Map<ShardId, OrderedDocCollector> collectorsByShardId = new HashMap<>(orderedDocCollectors.size());
        for (OrderedDocCollector orderedDocCollector : orderedDocCollectors) {
            collectorsByShardId.put(orderedDocCollector.shardId(), orderedDocCollector);
        }
        return collectorsByShardId;
    }
}
