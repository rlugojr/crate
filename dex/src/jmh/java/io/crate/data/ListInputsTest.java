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

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class ListInputsTest {

    private Supplier<Integer> get10 = () -> 10;
    private Input<Integer> input10 = new Input.IntInput(10);


    @Benchmark
    public Input<?> measureFromSuppliers() {
        InputList inputList = InputList.fromSuppliers(() -> 10);
        return inputList.get(0);
    }

    @Benchmark
    public Input<?> measureFromExistingSuppliers() {
        InputList inputList = InputList.fromSuppliers(get10);
        return inputList.get(0);
    }

    @Benchmark
    public void measureFromExistingSuppliersRepeatedGet(Blackhole blackhole) {
        InputList inputList = InputList.fromSuppliers(get10);
        for (int i = 0; i < 10_000_000; i++) {
            blackhole.consume(inputList.get(0));
        }
    }

    @Benchmark
    public void measureFromExistingSuppliersCreatingListInputsRepeatedGet(Blackhole blackhole) {
        InputList inputList = InputList.fromSuppliersCreatingListInputs(get10);
        for (int i = 0; i < 10_000_000; i++) {
            blackhole.consume(inputList.get(0));
        }
    }

    @Benchmark
    public Input<?> measureListInputsFromExistingInput() {
        InputList inputList = new InputList.ListInputs(input10);
        return inputList.get(0);
    }

    @Benchmark
    public void measureListInputsFromExistingInputRepeatedGet(Blackhole blackhole) {
        InputList inputList = new InputList.ListInputs(input10);
        for (int i = 0; i < 10_000_000; i++) {
            blackhole.consume(inputList.get(0));
        }
    }

    @Benchmark
    public Input<?> measureFromSupplierFromExistingInput() {
        InputList inputList = InputList.fromSuppliers(input10::value);
        return inputList.get(0);
    }

    @Benchmark
    public Input<?> measureListInputs() {
        InputList inputList = new InputList.ListInputs(() -> 10);
        return inputList.get(0);
    }

    @Benchmark
    public Input<?> measureListInputsNonLambda() {
        InputList inputList = new InputList.ListInputs(new Input.IntInput(10));
        return inputList.get(0);
    }


}
