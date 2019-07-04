/*
 * Copyright 2019 Ververica GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flinktraining.examples.datastream_java.basics.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * A parallel, finite/bounded source function, which emits the records given to it during construction.
 */
public class ParallelCollectionSource<T> extends RichParallelSourceFunction<T> {

  private List<T> input;
  private List<T> inputOfSubtask;

  public ParallelCollectionSource(List<T> input) {
    this.input = input;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    int numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
    int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();

    inputOfSubtask = new ArrayList<>();

    for (int i = 0; i < input.size(); i++) {
      if (i % numberOfParallelSubtasks == indexOfThisSubtask) {
        inputOfSubtask.add(input.get(i));
      }
    }
  }

  @Override
  public void run(SourceContext<T> ctx) throws Exception {
    for (T l : inputOfSubtask) {
      ctx.collect(l);
    }
  }

  @Override
  public void cancel() {
    // ignore cancel, finite anyway
  }
}
