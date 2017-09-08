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

package com.dataartisans.flinktraining.exercises.datastream_java.process;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.CheckpointedTaxiRideSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * Java reference implementation for the "Long Ride Alerts" exercise of the Flink training
 * (http://training.data-artisans.com).
 *
 * The goal for this exercise is to emit START events for taxi rides that have not been matched
 * by an END event during the first 2 hours of the ride.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
public class LongRides {
	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.getRequired("input");

		// 600 times faster than real time, i.e., 10 minutes of events are served every second
		final int servingSpeedFactor = 600;

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// set up checkpointing
		env.setStateBackend(new FsStateBackend("file:///tmp/checkpoints"));
		env.enableCheckpointing(1000);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(60, Time.of(10, TimeUnit.SECONDS)));

		// CheckpointedTaxiRideSource delivers events in order
		DataStream<TaxiRide> rides = env.addSource(new CheckpointedTaxiRideSource(input, servingSpeedFactor));

		DataStream<TaxiRide> longRides = rides
				.keyBy("rideId")
				.process(new MatchFunction());

		longRides.print();

		env.execute("Long Taxi Rides");
	}

	public static class MatchFunction extends ProcessFunction<TaxiRide, TaxiRide> {
		// keyed, managed state -- matching START and END taxi ride events
		private ValueState<TaxiRide> rideStartedState;
		private ValueState<TaxiRide> rideEndedState;

		@Override
		public void open(Configuration config) {
			ValueStateDescriptor<TaxiRide> startDescriptor =
					new ValueStateDescriptor<>("started-ride", TaxiRide.class);
			rideStartedState = getRuntimeContext().getState(startDescriptor);

			ValueStateDescriptor<TaxiRide> endDescriptor =
					new ValueStateDescriptor<>("ended-ride", TaxiRide.class);

			rideEndedState = getRuntimeContext().getState(endDescriptor);
		}

		@Override
		public void processElement(TaxiRide ride, Context context, Collector<TaxiRide> out) throws Exception {
			TimerService timerService = context.timerService();

			if (ride.isStart) {
				rideStartedState.update(ride);
				// set a timer for 120 event-time minutes after the ride started
				timerService.registerEventTimeTimer(ride.getEventTime() + 120 * 60 * 1000);
			} else {
				if (rideStartedState.value() != null) {
					rideEndedState.update(ride);
				} else {
					// There either was no matching START event, or
					// this is a long ride and the START has already been reported and cleared.
					// In either case, we should not create any state, since it will never get cleared.
				}
			}
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext context, Collector<TaxiRide> out) throws Exception {
			TaxiRide rideStarted = rideStartedState.value();
			TaxiRide rideEnded = rideEndedState.value();
			if (rideStarted != null && rideEnded == null) {
				out.collect(rideStarted);
			}
			rideStartedState.clear();
			rideEndedState.clear();
		}
	}
}