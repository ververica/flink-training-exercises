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

package com.ververica.flinktraining.examples.table_java.stream;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Instant;
import java.util.Random;

/*
	This is an example of how to sort an out-of-order stream, based on event time timestamps
	and watermarks, using the Table/SQL library.
 */

public class Sort {

	public static final int OUT_OF_ORDERNESS = 1000;

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		DataStream<Event> eventStream = env.addSource(new OutOfOrderEventSource())
				.assignTimestampsAndWatermarks(new TimestampsAndWatermarks(Time.milliseconds(OUT_OF_ORDERNESS)));

		Table events = tableEnv.fromDataStream(eventStream, "eventTime.rowtime");
		tableEnv.registerTable("events", events);
		Table sorted = tableEnv.sqlQuery("SELECT eventTime FROM events ORDER BY eventTime ASC");
		DataStream<Row> sortedEventStream = tableEnv.toAppendStream(sorted, Row.class);

		sortedEventStream.print();

		env.execute();
	}

	public static class Event {
		public Long eventTime;

		Event() {
			this.eventTime = Instant.now().toEpochMilli() + (new Random().nextInt(OUT_OF_ORDERNESS));
		}
	}

	private static class OutOfOrderEventSource implements SourceFunction<Event> {
		private volatile boolean running = true;

		@Override
		public void run(SourceContext<Event> ctx) throws Exception {
			while(running) {
				ctx.collect(new Event());
				Thread.sleep(1);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	private static class TimestampsAndWatermarks extends BoundedOutOfOrdernessTimestampExtractor<Event> {
		public TimestampsAndWatermarks(Time t) {
			super(t);
		}

		@Override
		public long extractTimestamp(Event event) {
			return event.eventTime;
		}
	}
}
