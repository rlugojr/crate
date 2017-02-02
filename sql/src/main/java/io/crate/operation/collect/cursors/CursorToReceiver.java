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

import io.crate.operation.data.BatchConsumer;
import io.crate.operation.data.BatchCursor;
import io.crate.operation.projectors.RepeatHandle;
import io.crate.operation.projectors.RowReceiver;

public class CursorToReceiver implements BatchConsumer {

    private final RowReceiver rowReceiver;

    public CursorToReceiver(RowReceiver rowReceiver) {
        this.rowReceiver = rowReceiver;
    }

    @Override
    public void accept(BatchCursor cursor, Throwable failure) {
        if (failure == null) {
            consumeCursor(cursor);
        } else {
            rowReceiver.fail(failure);
        }
    }

    private void consumeCursor(BatchCursor cursor) {
        switch (cursor.status()) {
            case ON_ROW:
                do {
                    RowReceiver.Result result = rowReceiver.setNextRow(cursor);
                    switch (result) {
                        case CONTINUE:
                            break;
                        case PAUSE:
                            rowReceiver.pauseProcessed(async -> consumeCursor(cursor));
                            return;
                        case STOP:
                            rowReceiver.finish(RepeatHandle.UNSUPPORTED);
                            cursor.close();
                            return;
                    }
                } while (cursor.moveNext());

                break;
            case OFF_ROW:
                if (cursor.allLoaded()) {
                    rowReceiver.finish(RepeatHandle.UNSUPPORTED);
                    cursor.close();
                    return;
                }
                cursor.loadNextBatch()
                    .thenAccept(i -> consumeCursor(cursor))
                    .exceptionally(e -> {
                        rowReceiver.fail(e);
                        cursor.close();
                        return null;
                    });
                break;
            case LOADING:
                break;
            case CLOSED:
                break;
        }
    }
}
