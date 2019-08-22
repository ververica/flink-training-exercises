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

package com.ververica.flinktraining.examples.datastream_java.cep;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Random;

/*
	This is an example of how to sort an out-of-order stream, based on event time timestamps
	and watermarks, using the CEP library.

	Note that with CEP, it's possible to gather any late events in a side output stream.
 */

public class Sort {

	public static final int OUT_OF_ORDERNESS = 1000;

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		DataStream<Event> eventStream = env.addSource(new OutOfOrderEventSource())
				.assignTimestampsAndWatermarks(new TimestampsAndWatermarks());

		Pattern<Event, ?> matchEverything =
				Pattern.<Event>begin("any")
						.where(new SimpleCondition<Event>() {
							@Override
							public boolean filter(Event event) throws Exception {
								return true;
							}
						});

		PatternStream<Event> patternStream = CEP.pattern(eventStream, matchEverything);
		OutputTag<Event> lateDataOutputTag = new OutputTag<Event>("late-events"){};

		SingleOutputStreamOperator<Event> sorted = patternStream
				.sideOutputLateData(lateDataOutputTag)
				.select(new PatternSelectFunction<Event, Event>() {
					@Override
					public Event select(Map<String, List<Event>> map) throws Exception {
						return map.get("any").get(0);
					}
				});

		sorted.print();
		sorted
				.getSideOutput(lateDataOutputTag)
				.map(e -> new Tuple2<>(e, "LATE"))
				.returns(Types.TUPLE(TypeInformation.of(Event.class), Types.STRING))
				.print();

		env.execute();
	}

	public static class Event {
		public Long ts;

		Event() {
			this.ts = Instant.now().toEpochMilli() + (new Random().nextInt(OUT_OF_ORDERNESS));
		}

		@Override
		public String toString() {
			return "Event@ " + ts;
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
		public TimestampsAndWatermarks() {
			super(Time.milliseconds(OUT_OF_ORDERNESS / 2));
		}

		@Override
		public long extractTimestamp(Event event) {
			return event.ts;
		}
	}
}

