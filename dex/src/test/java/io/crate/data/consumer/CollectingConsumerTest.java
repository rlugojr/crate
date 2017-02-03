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

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class CollectingConsumerTest {

    @Test
    public void testCollectingConsumer() throws Exception {
        StaticDataSource source = new StaticDataSource(Buckets.of(new Row1(10)));
        CollectingConsumer consumer = new CollectingConsumer(source);
        List<Object[]> objects = consumer.collect().get(10, TimeUnit.SECONDS);
        assertThat(objects.size(), is(1));
    }

    @Test
    public void testLimit() throws Exception {
        DataSource source = new StaticDataSource(new CollectionBucket(Arrays.asList(
            new Object[] { 1 },
            new Object[] { 2 },
            new Object[] { 3 },
            new Object[] { 4 },
            new Object[] { 5 }
        )));
        // TODO: this only works if the consumer doesn't call loadNext too often
        source = new TransformingDataSource(source, b -> new Bucket() {
            @Override
            public int size() {
                return 3;
            }

            @Override
            public Iterator<Row> iterator() {
                return Iterators.limit(b.iterator(), 3);
            }
        });
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
