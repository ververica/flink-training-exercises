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

package com.ververica.flinktraining.examples.datastream_java.windows;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Instant;
import java.util.Random;

/*

	When PARTITIONS_PER_INSTANCE is greater than 1, this job will report having lots of late events.

	(1) Why are there no late events when PARTITIONS_PER_INSTANCE == 1?

	(2) There are several different ways to fix this job so that it never has late events, even
	when PARTITIONS_PER_INSTANCE is greater than 1. Which approach seems most appropriate?

*/

public class WhyLate {
	public final static int PARTITIONS_PER_INSTANCE = 3;

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		OutputTag<Event> lateDataTag = new OutputTag<Event>("late"){};

		// When PARTITIONS_PER_INSTANCE is greater than 1, there can be late events.
		DataStream<Event> events = env.addSource(new ParallelEventSource(PARTITIONS_PER_INSTANCE));

		// Count the number of events per user in one second event-time windows,
		// and capture late events on a side output.
		SingleOutputStreamOperator<Tuple2<String, Integer>> windowOperator = events
				.assignTimestampsAndWatermarks(new TimestampsAndWatermarks())
				.keyBy(e -> e.userId)
				.window(TumblingEventTimeWindows.of(Time.seconds(1)))
				.sideOutputLateData(lateDataTag)
				.process(new CountEventsPerUser());

		windowOperator.print();

		// Count the number of late events for every second of processing time.pri
		windowOperator.getSideOutput(lateDataTag)
				.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1)))
				.process(new CountLateEvents())
				.map(i -> new Tuple3<String, Integer, String>("LATE", i, Instant.now().toString()))
				.returns(Types.TUPLE(Types.STRING, Types.INT, Types.STRING))
				.print();

		env.execute();
	}

	private static class Event {
		public final long timestamp;
		public final String userId;
		public final long partition;

		Event(long partition) {
			this.timestamp = Instant.now().toEpochMilli() + (100 * partition);
			this.userId = "U" + new Random().nextInt(6);
			this.partition = partition;
		}

		@Override
		public String toString() {
			return "Event{" + "userId=" + userId + ", partition=" + partition + ", @" + timestamp + '}';
		}
	}

	private static class ParallelEventSource extends RichParallelSourceFunction<Event> {
		private volatile boolean running = true;
		private transient long instance;
		private final int partitionsPerInstance;

		public ParallelEventSource(int partitionsPerInstance) {
			this.partitionsPerInstance = partitionsPerInstance;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			instance = getRuntimeContext().getIndexOfThisSubtask();
		}

		@Override
		public void run(SourceContext<Event> ctx) throws Exception {
			while(running) {
				for (int i = 0; i < partitionsPerInstance; i++) {
					ctx.collect(new Event(partitionsPerInstance * instance + i));
				}
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	private static class TimestampsAndWatermarks extends AscendingTimestampExtractor<Event> {
		@Override
		public long extractAscendingTimestamp(Event event) {
			return event.timestamp;
		}
	}

	private static class CountEventsPerUser extends ProcessWindowFunction<Event, Tuple2<String, Integer>, String, TimeWindow> {
		@Override
		public void process(
				String key,
				Context context,
				Iterable<Event> events,
				Collector<Tuple2<String, Integer>> out) throws Exception {

			int counter = 0;
			for (Object i : events) {
				counter++;
			}
			out.collect(new Tuple2<>(key, counter));
		}
	}

	private static class CountLateEvents extends ProcessAllWindowFunction<Event, Integer, TimeWindow> {
		@Override
		public void process(
				Context context,
				Iterable<Event> events,
				Collector<Integer> out) throws Exception {

			int counter = 0;
			for (Object i : events) {
				counter++;
			}
			out.collect(counter);
		}
	}
}
