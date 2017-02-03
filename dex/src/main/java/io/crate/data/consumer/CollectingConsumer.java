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

import io.crate.data.DataSource;
import io.crate.data.Page;
import io.crate.data.Row;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class CollectingConsumer {

    private final CompletableFuture<List<Object[]>> resultFuture = new CompletableFuture<>();
    private final List<Object[]> rows = new ArrayList<>();
    private final DataSource dataSource;

    public CollectingConsumer(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public CompletableFuture<List<Object[]>> collect() {
        dataSource.loadFirst().whenComplete(this::receivePage);
        return resultFuture;
    }

    private void receivePage(Page page, Throwable throwable) {
        if (throwable == null) {
            consumePage(page);
        } else {
            resultFuture.completeExceptionally(throwable);
        }
    }

    private void consumePage(Page page) {
        for (Row row : page.bucket()) {
            rows.add(row.materialize());
        }
        if (page.isLast()) {
            resultFuture.complete(Collections.unmodifiableList(rows));
        } else {
            page.loadNext().whenComplete(this::receivePage);
        }
    }
}
