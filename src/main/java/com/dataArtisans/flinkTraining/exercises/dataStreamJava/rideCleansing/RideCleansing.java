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

package com.dataArtisans.flinkTraining.exercises.dataStreamJava.rideCleansing;

import com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils.GeoUtils;
import com.dataArtisans.flinkTraining.exercises.dataStreamJava.dataTypes.TaxiRide;
import com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils.TaxiRideGenerator;
import com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils.TaxiRideSchema;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSink;

public class RideCleansing {

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		String input = params.getRequired("input");
		float servingSpeedFactor = params.getFloat("speed", 1.0f);

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// start the data generator
		DataStream<TaxiRide> rides = env.addSource(new TaxiRideGenerator(input, servingSpeedFactor));

		DataStream<TaxiRide> filteredRides = rides
				// filter out rides that do not start or stop in NYC
				.filter(new NYCFilter());

		// write the filtered data to a Kafka sink
		filteredRides.addSink(new KafkaSink<TaxiRide>("localhost:9092", "cleansedRides", new TaxiRideSchema()));

		// run the cleansing pipeline
		env.execute("Taxi Ride Cleansing");
	}


	public static class NYCFilter implements FilterFunction<TaxiRide> {

		@Override
		public boolean filter(TaxiRide taxiRide) throws Exception {

			return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
					GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
		}
	}

}
