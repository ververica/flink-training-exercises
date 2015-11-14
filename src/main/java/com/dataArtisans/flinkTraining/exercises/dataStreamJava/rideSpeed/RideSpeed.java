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

package com.dataArtisans.flinkTraining.exercises.dataStreamJava.rideSpeed;

import com.dataArtisans.flinkTraining.exercises.dataStreamJava.dataTypes.TaxiRide;
import com.dataArtisans.flinkTraining.exercises.dataStreamJava.rideCleansing.RideCleansing;
import com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils.TaxiRideGenerator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * Java reference implementation for the "Ride Speed" exercise of the Flink training (http://dataartisans.github.io/flink-training).
 * The task of the exercise is to compute the average speed of completed taxi rides from a data stream of taxi ride records.
 *
 * Parameters:
 *   --input path-to-input-directory
 *   --speed serving-speed-of-generator
 *
 */
public class RideSpeed {

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		String input = params.getRequired("input");
		float servingSpeedFactor = params.getFloat("speed", 1.0f);

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// start the data generator
		DataStream<TaxiRide> rides = env.addSource(new TaxiRideGenerator(input, servingSpeedFactor));

		DataStream<Tuple2<Long, Float>> rideSpeeds = rides
				// filter out rides that do not start or stop in NYC
				.filter(new RideCleansing.NYCFilter())
				// group records by rideId
				.keyBy("rideId")
				// match ride start and end records
				.flatMap(new RideEventJoiner())
				// compute the average speed of a ride
				.map(new SpeedComputer());

		// emit the result on stdout
		rideSpeeds.print();

		// run the transformation pipeline
		env.execute("Average Ride Speed");
	}

	/**
	 * Matches start and end TaxiRide records.
	 */
	public static class RideEventJoiner implements FlatMapFunction<TaxiRide, Tuple2<TaxiRide, TaxiRide>> {

		private HashMap<Long, TaxiRide> startRecords = new HashMap<Long, TaxiRide>();
		private Tuple2<TaxiRide, TaxiRide> joinedEvents = new Tuple2<TaxiRide, TaxiRide>();

		@Override
		public void flatMap(TaxiRide rideEvent, Collector<Tuple2<TaxiRide, TaxiRide>> out) throws Exception {

			if(rideEvent.isStart) {
				// remember start record
				startRecords.put(rideEvent.rideId, rideEvent);
			}
			else {
				// get and forget start record
				TaxiRide startRecord = startRecords.remove(rideEvent.rideId);
				if(startRecord != null) {
					// return start and end record
					joinedEvents.f0 = startRecord;
					joinedEvents.f1 = rideEvent;
					out.collect(joinedEvents);
				}
			}
		}
	}

	/**
	 * Computes the average speed of a taxi ride from its start and end record.
	 */
	public static class SpeedComputer implements MapFunction<Tuple2<TaxiRide, TaxiRide>, Tuple2<Long, Float>> {

		private static int MILLIS_PER_HOUR = 1000 * 60 * 60;
		private Tuple2<Long, Float> outT = new Tuple2<Long, Float>();

		@Override
		public Tuple2<Long, Float> map(Tuple2<TaxiRide, TaxiRide> joinedEvents) throws Exception {

			float distance = joinedEvents.f1.travelDistance;
			long startTime = joinedEvents.f0.time.getMillis();
			long endTime = joinedEvents.f1.time.getMillis();

			float speed;
			long timeDiff = endTime - startTime;
			if(timeDiff != 0) {
				// speed = distance / time
				speed = (distance / timeDiff) * MILLIS_PER_HOUR;
			}
			else {
				speed = -1;
			}

			// set ride Id
			outT.f0 = joinedEvents.f0.rideId;
			// compute speed
			outT.f1 = speed;

			return outT;
		}
	}


}
