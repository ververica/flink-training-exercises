/*
 * Copyright 2017 data Artisans GmbH, 2019 Ververica GmbH
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

import com.ververica.flinktraining.exercises.datastream_java.datatypes.ConnectedCarEvent;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.StoppedSegment;
import com.ververica.flinktraining.exercises.datastream_java.utils.ConnectedCarAssigner;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Java reference implementation for the "Driving Segments" exercise of the Flink training
 * (http://training.ververica.com).
 *
 * The task of the exercise is to divide the input stream of ConnectedCarEvents into segments,
 * where the car is being continuously driven without stopping.
 *
 * Parameters:
 * -input path-to-input-file
 */
public class DrivingSegments {

	public static void main(String[] args) throws Exception {

		// read parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		String input = params.getRequired("input");

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// connect to the data file
		DataStream<String> carData = env.readTextFile(input);

		// map to events
		DataStream<ConnectedCarEvent> events = carData
				.map((String line) -> ConnectedCarEvent.fromString(line))
				.assignTimestampsAndWatermarks(new ConnectedCarAssigner());

		// find segments
		events.keyBy("carId")
				.window(GlobalWindows.create())
				.trigger(new SegmentingOutOfOrderTrigger())
				.evictor(new SegmentingEvictor())
				.apply(new CreateStoppedSegment())
				.print();

		env.execute("Driving Segments");
	}

	public static class SegmentingOutOfOrderTrigger extends Trigger<ConnectedCarEvent, GlobalWindow> {

		@Override
		public TriggerResult onElement(ConnectedCarEvent event, long timestamp, GlobalWindow window, TriggerContext context) throws Exception {

			// if this is a stop event, set a timer
			if (event.speed == 0.0) {
				context.registerEventTimeTimer(event.timestamp);
			}

			return TriggerResult.CONTINUE;
		}

		@Override
		public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) {
			return TriggerResult.FIRE;
		}

		@Override
		public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) {
			return TriggerResult.CONTINUE;
		}

		@Override
		public void clear(GlobalWindow window, TriggerContext ctx) {
		}
	}

	public static class SegmentingEvictor implements Evictor<ConnectedCarEvent, GlobalWindow> {

		@Override
		public void evictBefore(Iterable<TimestampedValue<ConnectedCarEvent>> events,
								int size, GlobalWindow window, EvictorContext ctx) {
		}

		@Override
		public void evictAfter(Iterable<TimestampedValue<ConnectedCarEvent>> elements, int size, GlobalWindow window, EvictorContext ctx) {
			long firstStop = ConnectedCarEvent.earliestStopElement(elements);

			// remove all events up to (and including) the first stop event (which is the event that triggered the window)
			for (Iterator<TimestampedValue<ConnectedCarEvent>> iterator = elements.iterator(); iterator.hasNext(); ) {
				TimestampedValue<ConnectedCarEvent> element = iterator.next();
				if (element.getTimestamp() <= firstStop) {
					iterator.remove();
				}
			}
		}
	}

	public static class CreateStoppedSegment implements WindowFunction<ConnectedCarEvent, StoppedSegment, Tuple, GlobalWindow> {
		@Override
		public void apply(Tuple key, GlobalWindow window, Iterable<ConnectedCarEvent> events, Collector<StoppedSegment> out) {
			StoppedSegment seg = new StoppedSegment(events);
			if (seg.length > 0) {
				out.collect(seg);
			}
		}

	}

}
