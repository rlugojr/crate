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

import io.crate.testing.BatchIteratorTester;
import io.crate.testing.BatchSimulatingIterator;
import io.crate.testing.RowGenerator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CompositeBatchIteratorTest {

    private Iterable<Row> rows1 = RowGenerator.range(0, 5);
    private Iterable<Row> rows2 = RowGenerator.range(5, 10);

    private List<Object[]> expectedResult = Stream.concat(
        StreamSupport.stream(rows1.spliterator(), false),
        StreamSupport.stream(rows2.spliterator(), false))
        .map(Row::materialize)
        .collect(Collectors.toList());

    @Test
    public void testCompositeBatchIterator() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> new CompositeBatchIterator(RowsBatchIterator.newInstance(rows1), RowsBatchIterator.newInstance(rows2)),
            expectedResult
        );
        tester.run();
    }

    @Test
    public void testCompositeBatchIteratorWithBatchedSources() throws Exception {
        List<Object[]> expectedResult = new ArrayList<>();
        // consumes the unbatched/loaded iterator first
        expectedResult.add(new Object[] { 5L });
        expectedResult.add(new Object[] { 6L });
        expectedResult.add(new Object[] { 7L });
        expectedResult.add(new Object[] { 8L });
        expectedResult.add(new Object[] { 9L });
        expectedResult.add(new Object[] { 0L });
        expectedResult.add(new Object[] { 1L });
        expectedResult.add(new Object[] { 2L });
        expectedResult.add(new Object[] { 3L });
        expectedResult.add(new Object[] { 4L });
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> new CompositeBatchIterator(
                new CloseAssertingBatchIterator(new BatchSimulatingIterator(RowsBatchIterator.newInstance(rows1), 2, 6, null)),
                new CloseAssertingBatchIterator(RowsBatchIterator.newInstance(rows2))
            ),
            expectedResult
        );
        tester.run();
    }
}
