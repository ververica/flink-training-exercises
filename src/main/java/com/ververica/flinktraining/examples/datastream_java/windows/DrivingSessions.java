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
import com.ververica.flinktraining.exercises.datastream_java.datatypes.GapSegment;
import com.ververica.flinktraining.exercises.datastream_java.utils.ConnectedCarAssigner;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Java reference implementation for the "Driving Sessions" exercise of the Flink training
 * (http://training.ververica.com).
 * <p>
 * The task of the exercise is to divide the input stream of ConnectedCarEvents into
 * session windows, defined by a gap of 15 seconds. Each window should then be mapped onto
 * a GapSegment object.
 * <p>
 * Parameters:
 * -input path-to-input-file
 */

public class DrivingSessions {

	public static void main(String[] args) throws Exception {

		// read parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		String input = params.getRequired("input");

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// connect to the data file
		DataStream<String> carData = env.readTextFile(input);

		// find segments
		DataStream<ConnectedCarEvent> events = carData
				.map((String line) -> ConnectedCarEvent.fromString(line))
				.assignTimestampsAndWatermarks(new ConnectedCarAssigner());

		events.keyBy("carId")
				.window(EventTimeSessionWindows.withGap(Time.seconds(15)))
				.apply(new CreateGapSegment())
				.print();

		env.execute("Driving Sessions");
	}

	public static class CreateGapSegment implements WindowFunction<ConnectedCarEvent, GapSegment, Tuple, TimeWindow> {
		@Override
		public void apply(Tuple key, TimeWindow window, Iterable<ConnectedCarEvent> events, Collector<GapSegment> out) {
			out.collect(new GapSegment(events));
		}

	}
}
