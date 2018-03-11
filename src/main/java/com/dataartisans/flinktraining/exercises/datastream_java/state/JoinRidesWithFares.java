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

package com.dataartisans.flinktraining.exercises.datastream_java.state;

//import com.dataartisans.flinktraining.exercises.datastream_java.basics.RideCleansing;
import com.dataartisans.flinktraining.exercises.datastream_java.basics.RideCleansing;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.CheckpointedTaxiFareSource;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.CheckpointedTaxiRideSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Java reference implementation for the "Join Rides with Fares" exercise of the Flink training
 * (http://training.data-artisans.com).
 *
 * The goal for this exercise is to enrich TaxiRides with fare information.
 *
 * Parameters:
 * -rides path-to-input-file
 * -fares path-to-input-file
 *
 */
public class JoinRidesWithFares {
	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String tripsFile = params.getRequired("trips");
		final String faresFile = params.getRequired("fares");

		final int servingSpeedFactor = 1800; 	// 30 minutes worth of events are served every second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<TaxiRide> rides = env
				.addSource(new CheckpointedTaxiRideSource(tripsFile, servingSpeedFactor))
				.filter(new FilterFunction<TaxiRide>() {
					@Override
					public boolean filter(TaxiRide ride) throws Exception {
						return !ride.isStart;
					}
				})
				.keyBy("rideId");
		DataStream<TaxiFare> fares = env
				.addSource(new CheckpointedTaxiFareSource(faresFile, servingSpeedFactor))
				.keyBy("rideId");

		DataStream<Tuple2<TaxiRide, TaxiFare>> enrichedRides = rides
				.connect(fares)
				.flatMap(new EnrichmentFunction());

		enrichedRides.print();

		env.execute("Join Rides with Fares (java RichCoFlatMap)");
	}

	public static class EnrichmentFunction extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {
		// keyed, managed state
		private ValueState<TaxiRide> rideState;
		private ValueState<TaxiFare> fareState;

		@Override
		public void open(Configuration config) {
			rideState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved ride", TaxiRide.class));
			fareState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved fare", TaxiFare.class));
		}

		@Override
		public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			TaxiFare fare = fareState.value();
			if (fare != null) {
				fareState.clear();
				out.collect(new Tuple2(ride, fare));
			} else {
				rideState.update(ride);
			}
		}

		@Override
		public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			TaxiRide ride = rideState.value();
			if (ride != null) {
				rideState.clear();
				out.collect(new Tuple2(ride, fare));
			} else {
				fareState.update(fare);
			}
		}
	}
}