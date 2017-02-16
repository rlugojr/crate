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

package io.crate.action.sql;

import io.crate.data.BatchConsumer;
import io.crate.data.BatchIterator;

import javax.annotation.Nullable;

public class ResultReceiverConsumer implements BatchConsumer {

    private final ResultReceiver resultReceiver;
    private final int maxRows;

    private long rowCount = 0;

    public ResultReceiverConsumer(ResultReceiver resultReceiver, int maxRows) {
        this.resultReceiver = resultReceiver;
        this.maxRows = maxRows;
    }

    @Override
    public void accept(BatchIterator iterator, @Nullable Throwable failure) {
        if (failure == null) {
            consumeIt(iterator);
        } else {
            resultReceiver.fail(failure);
        }
    }

    private void consumeIt(BatchIterator it) {
        while (it.moveNext()) {
            rowCount++;
            resultReceiver.setNextRow(it.currentRow());

            if (maxRows > 0 && rowCount % maxRows == 0) {
                resultReceiver.batchFinished();
                return;
            }
        }
        if (it.allLoaded()) {
            resultReceiver.allFinished(false);
        } else {
            it.loadNextBatch().whenComplete((r, t) -> {
                if (t == null) {
                    consumeIt(it);
                } else {
                    resultReceiver.fail(t);
                }
            });
        }
    }
}
