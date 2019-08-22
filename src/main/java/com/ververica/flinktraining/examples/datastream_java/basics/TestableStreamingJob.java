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

package com.ververica.flinktraining.examples.datastream_java.basics;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.ververica.flinktraining.examples.datastream_java.utils.RandomLongSource;

/*
 * Example showing how to make sources and sinks pluggable in your application code so
 * you can inject special test sources and test sinks in your tests.
 */

public class TestableStreamingJob {
	private SourceFunction<Long> source;
	private SinkFunction<Long> sink;

	public TestableStreamingJob(SourceFunction<Long> source, SinkFunction<Long> sink) {
		this.source = source;
		this.sink = sink;
	}

	public void execute() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Long> LongStream =
				env.addSource(source)
						.returns(TypeInformation.of(Long.class));

		LongStream
				.map(new IncrementMapFunction())
				.addSink(sink);

		env.execute();
	}

	public static void main(String[] args) throws Exception {
		TestableStreamingJob job = new TestableStreamingJob(new RandomLongSource(), new PrintSinkFunction<>());
		job.execute();
	}

	// While it's tempting for something this simple, avoid using anonymous classes or lambdas
	// for any business logic you might want to unit test.
	public class IncrementMapFunction implements MapFunction<Long, Long> {

		@Override
		public Long map(Long record) throws Exception {
			return record + 1 ;
		}
	}

}
