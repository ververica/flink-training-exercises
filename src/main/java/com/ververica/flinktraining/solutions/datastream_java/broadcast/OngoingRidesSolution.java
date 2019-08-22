/*
 * Copyright 2018 data Artisans GmbH, 2019 Ververica GmbH
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

package com.ververica.flinktraining.solutions.datastream_java.broadcast;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.annotation.Nullable;
import java.util.Locale;

/**
 * Java reference implementation for the "Ongoing Rides" exercise of the Flink training
 * (http://training.ververica.com).
 *
 * The goal of this exercise is to report on all taxis whose current ride has been ongoing
 * for at least n minutes whenever the broadcast stream is queried (with the value of n).
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
public class OngoingRidesSolution extends ExerciseBase {
	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToRideData);

		final int maxEventDelay = 60;       	// events are out of order by at most 60 seconds
		final int servingSpeedFactor = 600; 	// 10 minutes worth of events are served every second

		// In this simple case we need a broadcast state descriptor, but aren't going to
		// use it to store anything.
		final MapStateDescriptor<Long, Long> dummyBroadcastState = new MapStateDescriptor<>(
				"dummy",
				BasicTypeInfo.LONG_TYPE_INFO,
				BasicTypeInfo.LONG_TYPE_INFO
		);

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor));

		// add a socket source
		BroadcastStream<String> queryStream = env.socketTextStream("localhost", 9999)
				.assignTimestampsAndWatermarks(new QueryStreamAssigner())
				.broadcast(dummyBroadcastState);

		DataStream<TaxiRide> reports = rides
				.keyBy((TaxiRide ride) -> ride.taxiId)
				.connect(queryStream)
				.process(new QueryFunction());

		printOrTest(reports);

		env.execute("Ongoing Rides");
	}

	public static class QueryFunction extends KeyedBroadcastProcessFunction<Long, TaxiRide, String, TaxiRide> {
		private ValueStateDescriptor<TaxiRide> taxiDescriptor =
				new ValueStateDescriptor<>("saved ride", TaxiRide.class);
		private ValueState<TaxiRide> taxiState;

		@Override
		public void open(Configuration config) {
			// We use a ValueState<TaxiRide> to store the latest ride event for each taxi.
			taxiState = getRuntimeContext().getState(taxiDescriptor);
		}

		@Override
		public void processElement(TaxiRide ride, ReadOnlyContext ctx, Collector< TaxiRide> out) throws Exception {
			// For every taxi, let's store the most up-to-date information.
			// TaxiRide implements Comparable to make this easy.
			TaxiRide savedRide = taxiState.value();
			if (ride.compareTo(savedRide) > 0) {
				taxiState.update(ride);
			}
		}

		@Override
		public void processBroadcastElement(String msg, Context ctx, Collector<TaxiRide> out) throws Exception {
			DateTimeFormatter timeFormatter =
					DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC();

			Long thresholdInMinutes = Long.valueOf(msg);
			Long wm = ctx.currentWatermark();
			System.out.println("QUERY: " + thresholdInMinutes + " minutes at " + timeFormatter.print(wm));

			// Collect to the output all ongoing rides that started at least thresholdInMinutes ago.
			ctx.applyToKeyedState(taxiDescriptor, new KeyedStateFunction<Long, ValueState<TaxiRide>>() {
				@Override
				public void process(Long taxiId, ValueState<TaxiRide> taxiState) throws Exception {
					TaxiRide ride = taxiState.value();
					if (ride.isStart) {
						long minutes = (wm - ride.getEventTime()) / 60000;
						if (ride.isStart && (minutes >= thresholdInMinutes)) {
							out.collect(ride);
						}
					}
				}
			});
		}
	}

	// Once the two streams are connected, the Watermark of the KeyedBroadcastProcessFunction
	// operator will be the minimum of the Watermarks of the two connected streams. Our query stream
	// has a default Watermark at Long.MIN_VALUE, and this will hold back the event time clock of
	// the connected stream, unless we do something about it.
	public static class QueryStreamAssigner implements AssignerWithPeriodicWatermarks<String> {
		@Nullable
		@Override
		public Watermark getCurrentWatermark() {
			return Watermark.MAX_WATERMARK;
		}

		@Override
		public long extractTimestamp(String element, long previousElementTimestamp) {
			return 0;
		}
	}

}