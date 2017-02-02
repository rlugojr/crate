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

import io.crate.core.collections.Row;
import io.crate.operation.projectors.RepeatHandle;
import io.crate.operation.projectors.RowReceiver;

public class AltCursorToRowReceiver {

    private final RowReceiver rowReceiver;

    public AltCursorToRowReceiver(RowReceiver rowReceiver) {
        this.rowReceiver = rowReceiver;
    }

    public void accept(AltBatchCursor cursor, Throwable failure) {
        if (failure == null) {
            consumeCursor(cursor);
        } else {
            rowReceiver.fail(failure);
        }
    }

    private void consumeCursor(AltBatchCursor cursor) {
        while (cursor.state() == AltBatchCursor.State.ON_ROW) {
            Row row = cursor.next();
            RowReceiver.Result result = rowReceiver.setNextRow(row);
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
        }

        switch (cursor.state()) {
            case NEED_FETCH:
                cursor.loadNext();
                break;

            case NO_MORE_DATA:
                rowReceiver.finish(RepeatHandle.UNSUPPORTED);
                cursor.close();
                break;
        }
    }
}
