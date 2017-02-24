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

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.operation.merge.*;
import org.elasticsearch.index.shard.ShardId;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
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
        return manyShardsBatchIterator(orderedDocCollectors, rowComparator, requiresScroll);
    }

    private static BatchIterator manyShardsBatchIterator(List<OrderedDocCollector> orderedDocCollectors,
                                                         Comparator<Row> rowComparator,
                                                         boolean requiresScroll) {
        BooleanSupplier allExhausted = getAllExhaustedSupplier(orderedDocCollectors);
        Map<ShardId, OrderedDocCollector> collectorsByShardId = toMapByShardId(orderedDocCollectors);

        PagingIterator<ShardId, Row> pagingIterator = new SortedPagingIterator<>(rowComparator, requiresScroll);
        AtomicReference<BatchPagingIterator<ShardId>> batchItRef = new AtomicReference<>();
        Function<ShardId, Boolean> tryFetchMore = getTryFetchMoreFunction(
            orderedDocCollectors, allExhausted, batchItRef, pagingIterator, collectorsByShardId);
        BatchPagingIterator<ShardId> batchIt = new BatchPagingIterator<>(
            pagingIterator,
            tryFetchMore,
            allExhausted,
            () -> {
                for (OrderedDocCollector orderedDocCollector : orderedDocCollectors) {
                    orderedDocCollector.close();
                }
            }
        );
        batchItRef.set(batchIt);
        return batchIt;
    }

    private static Map<ShardId, OrderedDocCollector> toMapByShardId(List<OrderedDocCollector> orderedDocCollectors) {
        Map<ShardId, OrderedDocCollector> collectorsByShardId = new HashMap<>(orderedDocCollectors.size());
        for (OrderedDocCollector orderedDocCollector : orderedDocCollectors) {
            collectorsByShardId.put(orderedDocCollector.shardId(), orderedDocCollector);
        }
        return collectorsByShardId;
    }

    private static BooleanSupplier getAllExhaustedSupplier(List<OrderedDocCollector> orderedDocCollectors) {
        return () -> {
            for (OrderedDocCollector orderedDocCollector : orderedDocCollectors) {
                if (!orderedDocCollector.exhausted) {
                    return false;
                }
            }
            return true;
        };
    }

    private static Function<ShardId, Boolean> getTryFetchMoreFunction(List<OrderedDocCollector> orderedDocCollectors,
                                                                      BooleanSupplier allExhausted,
                                                                      AtomicReference<BatchPagingIterator<ShardId>> batchItRef,
                                                                      PagingIterator<ShardId, Row> pagingIterator,
                                                                      Map<ShardId, OrderedDocCollector> collectorsByShardId) {
        return shardId -> {
            if (allExhausted.getAsBoolean()) {
                return false;
            }
            if (shardId == null) {
                loadFromAllUnExhausted(pagingIterator, orderedDocCollectors);
            } else {
                loadMore(pagingIterator, collectorsByShardId.get(shardId));
            }
            if (allExhausted.getAsBoolean()) {
                pagingIterator.finish();
            }
            batchItRef.get().completeLoad(null);
            return true;
        };
    }

    private static BatchIterator singleShardBatchIterator(OrderedDocCollector orderedDocCollector, boolean requiresScroll) {
        PagingIterator<ShardId, Row> pagingIterator =
            requiresScroll ? PassThroughPagingIterator.repeatable() : PassThroughPagingIterator.oneShot();

        // TODO: should we add a PagingIteratorLoadedListenable/Listener to the BatchPagingIterator
        // to replace the atomicRef/ BatchPagingIterator.completeLoaded
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
                // TODO: should we use the executor here? Or do we assume that the consumer of the batchIterator
                // is already in a non-netty thread and blocking for disk I/O is okay here?
                rows = orderedDocCollector.call();
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

    private static void loadFromAllUnExhausted(PagingIterator<ShardId, Row> pagingIterator,
                                               List<OrderedDocCollector> orderedDocCollectors) {
        List<KeyIterable<ShardId, Row>> rowsList = new ArrayList<>();

        for (OrderedDocCollector orderedDocCollector : orderedDocCollectors) {
            if (orderedDocCollector.exhausted) {
                continue;
            }
            // TODO: probably want to keep the old pattern to run call in a separate thread for all but one docCollector
            try {
                rowsList.add(orderedDocCollector.call());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        pagingIterator.merge(rowsList);
    }

    private static void loadMore(PagingIterator<ShardId, Row> pagingIterator, OrderedDocCollector orderedDocCollector) {
        try {
            KeyIterable<ShardId, Row> keyIterable = orderedDocCollector.call();
            pagingIterator.merge(Collections.singletonList(keyIterable));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
