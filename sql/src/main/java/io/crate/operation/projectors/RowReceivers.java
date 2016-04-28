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

package io.crate.operation.projectors;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.operation.OperationListener;
import io.crate.operation.OperationMultiListener;
import io.crate.operation.OperationObserver;
import org.elasticsearch.common.collect.Tuple;

import javax.annotation.ParametersAreNonnullByDefault;

public class RowReceivers {

    public static ListenableRowReceiver listenableRowReceiver(RowReceiver rowReceiver) {
        if (rowReceiver instanceof ListenableRowReceiver) {
            return (ListenableRowReceiver) rowReceiver;
        }
        return new SettableFutureRowReceiver(rowReceiver);
    }

    public static Tuple<RowReceiver, OperationObserver> observedRowReceiver(RowReceiver rowReceiver) {
        rowReceiver = findLastRowReceiverOfPossibleProjector(rowReceiver);
        if (rowReceiver instanceof OperationObserver) {
            return new Tuple<>(rowReceiver, (OperationObserver) rowReceiver);
        }
        if (!rowReceiver.isSynchronous()) {
            throw new UnsupportedOperationException("Given RowReceiver implementation will not work synchronous and so cannot be wrapped");
        }
        ObservedRowReceiver observedRowReceiver = new ObservedRowReceiver(rowReceiver);
        return new Tuple<>((RowReceiver) observedRowReceiver, (OperationObserver) observedRowReceiver);
    }

    static RowReceiver findLastRowReceiverOfPossibleProjector(RowReceiver rowReceiver) {
        if (rowReceiver instanceof Projector) {
            return findLastRowReceiverOfPossibleProjector(((Projector) rowReceiver).downstream());
        }
        return rowReceiver;
    }

    @ParametersAreNonnullByDefault
    private static class SettableFutureRowReceiver extends ForwardingRowReceiver implements ListenableRowReceiver {

        private final SettableFuture<Void> finishedFuture = SettableFuture.create();

        SettableFutureRowReceiver(RowReceiver rowReceiver) {
            super(rowReceiver);
        }

        @Override
        public void finish() {
            super.finish();
            finishedFuture.set(null);
        }

        @Override
        public void fail(Throwable throwable) {
            super.fail(throwable);
            finishedFuture.setException(throwable);
        }

        @Override
        public ListenableFuture<Void> finishFuture() {
            return finishedFuture;
        }

        @Override
        public boolean isSynchronous() {
            return rowReceiver.isSynchronous();
        }
    }

    private static class ObservedRowReceiver extends ForwardingRowReceiver implements OperationObserver {

        OperationListener listener = OperationListener.NO_OP;

        ObservedRowReceiver(RowReceiver rowReceiver) {
            super(rowReceiver);
        }

        @Override
        public void finish() {
            super.finish();
            listener.onDone(null);
        }

        @Override
        public void fail(Throwable throwable) {
            super.fail(throwable);
            listener.onDone(throwable);
        }

        @Override
        public void addListener(OperationListener listener) {
            this.listener = OperationMultiListener.merge(this.listener, listener);
        }

        @Override
        public boolean isSynchronous() {
            return false;
        }
    }
}
