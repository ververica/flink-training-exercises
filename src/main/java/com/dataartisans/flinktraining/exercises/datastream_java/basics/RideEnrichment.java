/*
 * Copyright 2015 data Artisans GmbH
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

package com.dataartisans.flinktraining.exercises.datastream_java.basics;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Minutes;

/**
 * Java reference implementation for the "Ride Cleansing" exercise of the Flink training
 * (http://training.data-artisans.com).
 * The task of the exercise is to filter a data stream of taxi ride records to keep only rides that
 * start and end within New York City. The resulting stream should be printed.
 *
 * Parameters:
 *   -input path-to-input-file
 *
 */
public class RideEnrichment {

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.getRequired("input");

		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// start the data generator
		DataStream<TaxiRide> rides = env.addSource(
				new TaxiRideSource(input, maxEventDelay, servingSpeedFactor));

		DataStream<EnrichedRide> enrichedNYCRides = rides.flatMap(new NYCEnrichment());

		DataStream<Tuple2<Integer, Minutes>> minutesByStartCell = enrichedNYCRides
				.flatMap(new FlatMapFunction<EnrichedRide, Tuple2<Integer, Minutes>>() {
					@Override
					public void flatMap(EnrichedRide ride,
										Collector<Tuple2<Integer, Minutes>> out) throws Exception {
						if (!ride.isStart) {
							Interval rideInterval = new Interval(ride.startTime, ride.endTime);
							Minutes duration = rideInterval.toDuration().toStandardMinutes();
							out.collect(new Tuple2<>(ride.startCell, duration));
						}
					}
				});

		minutesByStartCell
//				.keyBy(0)
//				.maxBy(1)
//				.process(new ProcessFunction<Tuple2<Integer,Minutes>, Integer>() {
//					Integer max = 0;
//					@Override
//					public void processElement(Tuple2<Integer, Minutes> x, Context context, Collector<Integer> collector) throws Exception {
//						if (x.f1.getMinutes() > max) max = x.f1.getMinutes();
//						collector.collect(max);
//					}
//				})
//				.setParallelism(1)
				.countWindowAll(1)
				.max(1)
				.setParallelism(1)
				.print()
				.setParallelism(1);

		// run the enrichment pipeline
		env.execute("Taxi Ride Enrichment");
	}

	public static class EnrichedRide extends TaxiRide {
		public int startCell;
		public int endCell;

		public EnrichedRide() {}

		public EnrichedRide(TaxiRide ride) {
			this.rideId = ride.rideId;
			this.isStart = ride.isStart;
			this.startTime = ride.startTime;
			this.endTime = ride.endTime;
			this.startLon = ride.startLon;
			this.startLat = ride.startLat;
			this.endLon = ride.endLon;
			this.endLat = ride.endLat;
			this.passengerCnt = ride.passengerCnt;
			this.startCell = GeoUtils.mapToGridCell(ride.startLon, ride.startLat);
			this.endCell = GeoUtils.mapToGridCell(ride.endLon, ride.endLat);
		}

		public String toString() {
			return super.toString() + "," + Integer.toString(this.startCell) + "," + Integer.toString(this.endCell);
		}
	}

	public static class NYCEnrichment implements FlatMapFunction<TaxiRide, EnrichedRide> {
		@Override
		public void flatMap(TaxiRide taxiRide, Collector<EnrichedRide> collector) throws Exception {
			FilterFunction<TaxiRide> f = new RideCleansing.NYCFilter();
			if (f.filter(taxiRide)) {
				collector.collect(new EnrichedRide(taxiRide));
			}

		}
	}

}
