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

package io.crate.data.transform;

import io.crate.data.Bucket;
import io.crate.data.DataSource;
import io.crate.data.Page;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class TransformingDataSource implements DataSource {

    private final DataSource source;
    private final Function<Bucket, Bucket> bucketTransformation;

    public TransformingDataSource(DataSource source, Function<Bucket, Bucket> bucketTransformation) {
        this.source = source;
        this.bucketTransformation = bucketTransformation;
    }

    @Override
    public CompletableFuture<Page> loadFirst() {
        return source.loadFirst().thenApply(p -> new TransformingPage(p, bucketTransformation));
    }

    @Override
    public void close() {
        source.close();
    }

    private static class TransformingPage implements Page {

        private final Page page;
        private final Function<Bucket, Bucket> bucketTransformation;

        TransformingPage(Page page, Function<Bucket, Bucket> bucketTransformation) {
            this.page = page;
            this.bucketTransformation = bucketTransformation;
        }

        @Override
        public CompletableFuture<Page> loadNext() {
            return page.loadNext().thenApply(p -> new TransformingPage(p, bucketTransformation));
        }

        @Override
        public Bucket bucket() {
            return bucketTransformation.apply(page.bucket());
        }

        @Override
        public boolean isLast() {
            return page.isLast();
        }
    }
}
