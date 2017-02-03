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

import io.crate.breaker.RamAccountingContext;
import io.crate.operation.Input;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.data.BatchConsumer;
import io.crate.operation.data.BatchCursor;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class BatchDocCollector implements CrateCollector {

    private final ShardId shardId;
    private final IndexSearcher indexSearcher;
    private final Query query;
    private final Float minScore;
    private final Executor executor;
    private final boolean doScores;
    private final CollectorContext collectorContext;
    private final RamAccountingContext ramAccountingContext;
    private final BatchConsumer batchConsumer;
    private final List<Input<?>> inputs;
    private final Collection<? extends LuceneCollectorExpression<?>> expressions;
    private final CollectorCursor cursor;

    public BatchDocCollector(ShardId shardId,
                             IndexSearcher indexSearcher,
                             Query query,
                             Float minScore,
                             Executor executor,
                             boolean doScores,
                             CollectorContext collectorContext,
                             RamAccountingContext ramAccountingContext,
                             BatchConsumer batchConsumer,
                             List<Input<?>> inputs,
                             Collection<? extends LuceneCollectorExpression<?>> expressions) {
        this.shardId = shardId;
        this.indexSearcher = indexSearcher;
        this.query = query;
        this.minScore = minScore;
        this.executor = executor;
        this.doScores = doScores;
        this.collectorContext = collectorContext;
        this.ramAccountingContext = ramAccountingContext;
        this.batchConsumer = batchConsumer;
        this.inputs = inputs;
        this.expressions = expressions;
        this.cursor = new CollectorCursor();
    }

    @Override
    public void doCollect() {
        batchConsumer.accept(cursor, null);
    }

    @Override
    public void kill(@Nullable Throwable throwable) {
    }

    private static class CollectorCursor implements BatchCursor {

        @Override
        public boolean moveFirst() {
            return false;
        }

        @Override
        public boolean moveNext() {
            return false;
        }

        @Override
        public void close() {
        }

        @Override
        public Status status() {
            return Status.OFF_ROW;
        }

        @Override
        public CompletableFuture<?> loadNextBatch() {
            return null;
        }

        @Override
        public boolean allLoaded() {
            return false;
        }

        @Override
        public int numColumns() {
            return 0;
        }

        @Override
        public Object get(int index) {
            return null;
        }

        @Override
        public Object[] materialize() {
            return new Object[0];
        }
    }
}
