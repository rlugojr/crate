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

package io.crate.data.consumer;

import com.google.common.collect.Iterators;
import io.crate.data.*;
import io.crate.data.transform.TopNOrderBySource;
import io.crate.data.transform.TransformingDataSource;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class CollectingConsumerTest {

    private static class UnlimitedPage implements Page {

        private final int start;
        private final int size;
        private final ArrayList<Row> items;

        UnlimitedPage(int start, int size) {
            this.start = start;
            this.size = size;
            this.items = new ArrayList<>(size);
            for (int i = start; i < start + size; i++) {
                items.add(new Row1(i));
            }
        }

        @Override
        public CompletableFuture<Page> loadNext() {
            return CompletableFuture.completedFuture(new UnlimitedPage(start + size, size));
        }

        @Override
        public Iterable<Row> data() {
            return items;
        }

        @Override
        public boolean isLast() {
            return false;
        }
    }

    private static class UnlimitedSourceBuilder implements DataSource.SourceBuilder {

        int offset = 0;
        int limit = 0;

        @Override
        public DataSource.SourceBuilder skip(int offset) {
            this.offset = offset;
            return this;
        }

        @Override
        public DataSource.SourceBuilder limit(int limit) {
            this.limit = limit;
            return this;
        }

        @Override
        public DataSource.SourceBuilder filter(Predicate<Row> filter) {
            return this;
        }

        @Override
        public DataSource.SourceBuilder addTransformation(Function<Iterable<Row>, Iterable<Row>> transformation) {
            return this;
        }

        @Override
        public DataSource build() {
            return new UnlimitedDataSource();
        }
    }

    private static class UnlimitedDataSource implements DataSource {

        private final CompletableFuture<Page> firstPage;

        UnlimitedDataSource() {
            firstPage = CompletableFuture.completedFuture(new UnlimitedPage(0, 3));
        }

        @Override
        public CompletableFuture<Page> loadFirst() {
            return firstPage;
        }

        @Override
        public void close() {
        }
    }

    @Test
    public void testCollectingConsumer() throws Exception {
        StaticDataSource source = new StaticDataSource(Buckets.of(new Row1(10)));
        CollectingConsumer consumer = new CollectingConsumer(source);
        List<Object[]> objects = consumer.collect().get(10, TimeUnit.SECONDS);
        assertThat(objects.size(), is(1));
    }

    @Test
    public void testLimit() throws Exception {
        DataSource source = new UnlimitedDataSource();
        // TODO: need to push the limit somehow into the source

        source = new TransformingDataSource(source, b -> () -> Iterators.limit(b.iterator(), 3));
        CollectingConsumer consumer = new CollectingConsumer(source);
        List<Object[]> objects = consumer.collect().get(10, TimeUnit.SECONDS);
        assertThat(objects.size(), is(3));
    }

    @Test
    public void testOrderByLimit() throws Exception {
        DataSource source = new StaticDataSource(new CollectionBucket(Arrays.asList(
            new Object[] { 2 },
            new Object[] { 5 },
            new Object[] { 4 },
            new Object[] { 1 },
            new Object[] { 3 }
        )));
        source = new TopNOrderBySource(source, 3, Comparator.comparingInt(o -> (int) o[0]));
        CollectingConsumer consumer = new CollectingConsumer(source);
        List<Object[]> objects = consumer.collect().get(10, TimeUnit.SECONDS);
        assertThat(objects.size(), is(3));
    }
}
