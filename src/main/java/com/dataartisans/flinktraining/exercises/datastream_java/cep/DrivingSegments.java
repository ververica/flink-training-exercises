/*
 * Copyright 2017 data Artisans GmbH
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

package com.dataartisans.flinktraining.exercises.datastream_java.cep;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.ConnectedCarEvent;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.StoppedSegment;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ConnectedCarAssigner;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

/**
 * Java reference implementation for a CEP-based solution to the "Driving Segments" exercise of the Flink training
 * (http://training.data-artisans.com).
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
				.map(new MapFunction<String, ConnectedCarEvent>() {
					@Override
					public ConnectedCarEvent map(String line) throws Exception {
						return ConnectedCarEvent.fromString(line);
					}
				})
				.assignTimestampsAndWatermarks(new ConnectedCarAssigner())
				.keyBy("carId");

		Pattern<ConnectedCarEvent, ?> driving =
				Pattern.<ConnectedCarEvent>begin("stoppedBefore")
						.where(new SimpleCondition<ConnectedCarEvent>() {
							@Override
							public boolean filter(ConnectedCarEvent event) throws Exception {
								return event.speed == 0;
							}
						})
						.next("driving")
						.where(new SimpleCondition<ConnectedCarEvent>() {
							@Override
							public boolean filter(ConnectedCarEvent event) throws Exception {
								return event.speed != 0;
							}
						})
						.oneOrMore()
						.next("stoppedAfter")
						.where(new SimpleCondition<ConnectedCarEvent>() {
							@Override
							public boolean filter(ConnectedCarEvent event) throws Exception {
								return event.speed == 0;
							}
						});

		PatternStream<ConnectedCarEvent> patternStream = CEP.pattern(events, driving);

		patternStream.select(new SelectSegment()).print();

		env.execute("Driving Segments");
	}

	public static class SelectSegment implements PatternSelectFunction<ConnectedCarEvent, StoppedSegment> {
		public StoppedSegment select(Map<String, List<ConnectedCarEvent>> pattern) {
			return new StoppedSegment(pattern.get("driving"));
		}
	}

}
