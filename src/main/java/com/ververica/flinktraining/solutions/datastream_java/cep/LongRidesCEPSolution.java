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

package com.ververica.flinktraining.solutions.datastream_java.cep;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.CheckpointedTaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * Java/CEP reference implementation for the "Long Ride Alerts" exercise of the Flink training
 * (http://training.ververica.com).
 *
 * The goal for this exercise is to emit START events for taxi rides that have not been matched
 * by an END event during the first 2 hours of the ride.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
public class LongRidesCEPSolution extends ExerciseBase {
	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToRideData);

		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		// CheckpointedTaxiRideSource delivers events in order
		DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new CheckpointedTaxiRideSource(input, servingSpeedFactor)));

		DataStream<TaxiRide> keyedRides = rides
			.keyBy("rideId");

		// A complete taxi ride has a START event followed by an END event
		Pattern<TaxiRide, TaxiRide> completedRides =
				Pattern.<TaxiRide>begin("start")
						.where(new SimpleCondition<TaxiRide>() {
							@Override
							public boolean filter(TaxiRide ride) throws Exception {
								return ride.isStart;
							}
						})
						.next("end")
						.where(new SimpleCondition<TaxiRide>() {
							@Override
							public boolean filter(TaxiRide ride) throws Exception {
								return !ride.isStart;
							}
						});

		// We want to find rides that have NOT been completed within 120 minutes.
		// This pattern matches rides that ARE completed.
		// Below we will ignore rides that match this pattern, and emit those that timeout.
		PatternStream<TaxiRide> patternStream = CEP.pattern(keyedRides, completedRides.within(Time.minutes(120)));

		OutputTag<TaxiRide> timedout = new OutputTag<TaxiRide>("timedout"){};

		SingleOutputStreamOperator<TaxiRide> longRides = patternStream.flatSelect(
				timedout,
				new TaxiRideTimedOut<TaxiRide>(),
				new FlatSelectNothing<TaxiRide>()
		);

		printOrTest(longRides.getSideOutput(timedout));

		env.execute("Long Taxi Rides (CEP)");
	}

	public static class TaxiRideTimedOut<TaxiRide> implements PatternFlatTimeoutFunction<TaxiRide, TaxiRide> {
		@Override
		public void timeout(Map<String, List<TaxiRide>> map, long l, Collector<TaxiRide> collector) throws Exception {
			TaxiRide rideStarted = map.get("start").get(0);
			collector.collect(rideStarted);
		}
	}

	public static class FlatSelectNothing<T> implements PatternFlatSelectFunction<T, T> {
		@Override
		public void flatSelect(Map<String, List<T>> pattern, Collector<T> collector) {
		}
	}
}
