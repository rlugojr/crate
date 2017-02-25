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

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

/**
 * A list of inputs for use as accessors to the underlying data for each column of a row of data.
 */
public interface InputList extends Iterable<Input<?>> {

    /**
     * Returns an input object to be used as an accessor to the data of the column identified by its position.
     * <p>
     * Note that objects implementing this interface are required to always return the same input instance in such that
     * get(1) == get(1) is always true.
     *
     * @param index zero based position of the input
     * @return the input at the specified position
     * @throws IndexOutOfBoundsException if the index is out of range
     *                                   (<tt>index &lt; 0 || index &gt;= size()</tt>)
     */
    Input<?> get(int index);

    /**
     * Returns the number of inputs in this input list.
     *
     * @return the number of inputs
     */
    int size();

    InputList EMPTY = new InputList() {
        @Override
        public Input<?> get(int index) {
            throw new IndexOutOfBoundsException("No input found at index: " + index);
        }

        @Override
        public int size() {
            return 0;
        }
    };

    @Override
    default Iterator<Input<?>> iterator() {
        return new Iterator<Input<?>>() {
            int i = -1;

            @Override
            public boolean hasNext() {
                return i + 1 < size();
            }

            @Override
            public Input<?> next() {
                if (size() > i) {
                    return get(++i);
                }
                throw new NoSuchElementException("Iterator exhausted");
            }
        };
    }

    /**
     * Creates a new input list with a single column using the given supplier as data source.
     *
     * @param supplier the supplier providing the value
     * @param <T>      the type of object returned by the input
     * @return an input list object with one input
     */
    static <T> InputList singleCol(Supplier<T> supplier) {
        final Input<T> input = supplier::get;
        return new InputList() {
            @Override
            public Input<?> get(int index) {
                if (index != 0) {
                    throw new IndexOutOfBoundsException("No input found at index: " + index);

                }
                return input;
            }

            @Override
            public int size() {
                return 1;
            }
        };
    }

    /**
     * Creates a new input list which wraps list of inputs. Modifications to the passed in list must be prevented, since this
     * would break the immutability contract of this interface.
     *
     * @param listOfInputs the list of inputs to be wrapped
     * @return an input list
     */
    static InputList wrap(List<? extends Input<?>> listOfInputs) {
        return new InputList() {
            @Override
            public Input<?> get(int index) {
                return listOfInputs.get(index);
            }

            @Override
            public int size() {
                return listOfInputs.size();
            }
        };
    }
}
