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

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;

public interface DataSource {

    // CompletableFuture<Page> loadFirst(Function<Iterable<Row>, Iterable<Row>> ... transformations);
    // CompletableFuture<Page> loadFirst(int offset, int limit, Predicate<Row> filter);

    /**
     * PageBuilder prepareLoad()
     *      .limit(10)
     *      .load()
     */


    CompletableFuture<Page> loadFirst();

    void close();


    /**
     * Usage:
     *
     *  sourceBuilder = createSourceBuilder()  (will create e.g. a Lucene SourceBuilder)
     *
     *  for projection in projections:
     *                      // use skip/limit/filter/addTransformation
     *      sourceBuilder = applyProjection(projection, sourceBuilder)
     *
     *  source = sourceBuilder.build()
     *  for sourceTransformation in sourceTransformation:
     *      // projections like group by which consume the whole source to create
     *      // a new source become sourceTransformations
     *      source = sourceTransformation.transform(source)
     *
     *  // consumer: consumes all pages, result is void.
     *  // (Behaves like a DistributingDownstream, or postgres ResultSetReceiver)
     *
     *  consumer.consume(source)
     */

    interface Transformer {
        DataSource transform(DataSource source);
    }

    interface Builder {
        Builder skip(int offset);
        Builder limit(int limit);
        Builder filter(Predicate<Row> filter);
        Builder addTransformation(Function<Iterable<Row>, Iterable<Row>> transformation);

        DataSource build();
    }

    interface Consumer {
        void consume(DataSource source);
    }
}
