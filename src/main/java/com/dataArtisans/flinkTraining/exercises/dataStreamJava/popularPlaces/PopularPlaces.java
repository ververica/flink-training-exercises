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

package com.dataArtisans.flinkTraining.exercises.dataStreamJava.popularPlaces;

import com.dataArtisans.flinkTraining.exercises.dataStreamJava.rideCleansing.RideCleansing;
import com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils.GeoUtils;
import com.dataArtisans.flinkTraining.exercises.dataStreamJava.dataTypes.TaxiRide;
import com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils.TaxiRideGenerator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.WindowMapFunction;
import org.apache.flink.streaming.api.windowing.helper.Time;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class PopularPlaces {

	private static final long COUNT_WINDOW_LENGTH = 15 * 60 * 1000; // 15 minutes in msecs
	private static final long COUNT_WINDOW_FREQUENCY = 5 * 60 * 1000; // 5 minutes in msecs

	public static void main(String[] args) throws Exception {

		// read parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		String input = params.getRequired("input");
		int popThreashold = Integer.parseInt(params.getRequired("popThreshold"));
		float servingSpeedFactor = params.getFloat("speed", 1.0f);

		// adjust window size and eviction interval to fast-forward factor
		int windowSize = (int)(COUNT_WINDOW_LENGTH / servingSpeedFactor);
		int evictionInterval = (int)(COUNT_WINDOW_FREQUENCY / servingSpeedFactor);

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// start the data generator
		DataStream<TaxiRide> rides = env.addSource(new TaxiRideGenerator(input, servingSpeedFactor));

		// find n most popular spots
		DataStream<Tuple4<Float, Float, Boolean, Integer>> popularSpots = rides
				.filter(new RideCleansing.NYCFilter())
				.map(new GridCellMatcher())
				.groupBy(0, 1)
				.window(Time.of(windowSize, TimeUnit.MILLISECONDS)).every(Time.of(evictionInterval, TimeUnit.MILLISECONDS))
				.mapWindow(new PopularityCounter(popThreashold))
				.flatten()
				.map(new GridToCoordinates());

		// print result on stdout
		popularSpots.print();

		// execute the transformation pipeline
		env.execute("Popular Places");
	}

	/**
	 * MapFunction to map start / end location of a TaxiRide to a cell Id
	 */
	public static class GridCellMatcher implements MapFunction<TaxiRide, Tuple2<Integer, Boolean>> {

		@Override
		public Tuple2<Integer, Boolean> map(TaxiRide taxiRide) throws Exception {
			if(taxiRide.isStart) {
				int gridId = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);
				return new Tuple2<Integer, Boolean>(gridId, true);
			} else {
				int gridId = GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat);
				return new Tuple2<Integer, Boolean>(gridId, false);
			}
		}
	}

	/**
	 * WindowMapFunction to count starts or stops per grid cell.
	 */

	public static class PopularityCounter implements WindowMapFunction<Tuple2<Integer, Boolean>, Tuple3<Integer, Boolean, Integer>> {

		private int popThreshold;

		public PopularityCounter(int popThreshold) {
			this.popThreshold = popThreshold;
		}

		@Override
		public void mapWindow(Iterable<Tuple2<Integer, Boolean>> values, Collector<Tuple3<Integer, Boolean, Integer>> out) {

			Tuple3<Integer, Boolean, Integer> cellCount = new Tuple3<Integer, Boolean, Integer>();

			cellCount.f2 = 0;
			for (Tuple2<Integer, Boolean> value : values) {
				// grid id
				cellCount.f0 = value.f0;
				// arriving or departing
				cellCount.f1 = value.f1;
				// increase counter
				cellCount.f2++;
			}

			if(cellCount.f2 >= popThreshold) {
				out.collect(cellCount);
			}
		}

	}

	public static class GridToCoordinates implements MapFunction<Tuple3<Integer, Boolean, Integer>, Tuple4<Float, Float, Boolean, Integer>> {

		@Override
		public Tuple4<Float, Float, Boolean, Integer> map(Tuple3<Integer, Boolean, Integer> cellCount) throws Exception {

			return new Tuple4<Float, Float, Boolean, Integer>(
					GeoUtils.getGridCellCenterLon(cellCount.f0),
					GeoUtils.getGridCellCenterLat(cellCount.f0),
					cellCount.f1,
					cellCount.f2);
		}
	}

}
