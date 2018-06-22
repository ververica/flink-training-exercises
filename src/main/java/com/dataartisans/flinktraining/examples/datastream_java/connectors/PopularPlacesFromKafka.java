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

package com.dataartisans.flinktraining.examples.datastream_java.connectors;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TaxiRideSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * Java reference implementation for the "Popular Places" exercise of the Flink training
 * (http://training.data-artisans.com).
 *
 * The task of the exercise is to identify every five minutes popular areas where many taxi rides
 * arrived or departed in the last 15 minutes.
 * The input is read from a Kafka topic that containes cleansed taxi rides.
 *
 */
public class PopularPlacesFromKafka {

	private static final String LOCAL_ZOOKEEPER_HOST = "localhost:2181";
	private static final String LOCAL_KAFKA_BROKER = "localhost:9092";
	private static final String RIDE_SPEED_GROUP = "rideSpeedGroup";
	private static final int MAX_EVENT_DELAY = 60; // rides are at most 60 sec out-of-order.

	public static void main(String[] args) throws Exception {

		final int popThreshold = 20; // threshold for popular places

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setAutoWatermarkInterval(1000);

		// configure the Kafka consumer
		Properties kafkaProps = new Properties();
		kafkaProps.setProperty("zookeeper.connect", LOCAL_ZOOKEEPER_HOST);
		kafkaProps.setProperty("bootstrap.servers", LOCAL_KAFKA_BROKER);
		kafkaProps.setProperty("group.id", RIDE_SPEED_GROUP);
		// always read the Kafka topic from the start
		kafkaProps.setProperty("auto.offset.reset", "earliest");

		// create a Kafka consumer
		FlinkKafkaConsumer011<TaxiRide> consumer = new FlinkKafkaConsumer011<>(
				"cleansedRides",
				new TaxiRideSchema(),
				kafkaProps);
		// assign a timestamp extractor to the consumer
		consumer.assignTimestampsAndWatermarks(new TaxiRideTSExtractor());

		// create a TaxiRide data stream
		DataStream<TaxiRide> rides = env.addSource(consumer);

		// find popular places
		DataStream<Tuple5<Float, Float, Long, Boolean, Integer>> popularPlaces = rides
				// match ride to grid cell and event type (start or end)
				.map(new GridCellMatcher())
				// partition by cell id and event type
				.keyBy(0, 1)
				// build sliding window
				.timeWindow(Time.minutes(15), Time.minutes(5))
				// count ride events in window
				.apply(new RideCounter())
				// filter by popularity threshold
				.filter((Tuple4<Integer, Long, Boolean, Integer> count) -> (count.f3 >= popThreshold))
				// map grid cell to coordinates
				.map(new GridToCoordinates());

		popularPlaces.print();

		// execute the transformation pipeline
		env.execute("Popular Places from Kafka");
	}

	/**
	 * Assigns timestamps to TaxiRide records.
	 * Watermarks are a fixed time interval behind the max timestamp and are periodically emitted.
	 */
	public static class TaxiRideTSExtractor extends BoundedOutOfOrdernessTimestampExtractor<TaxiRide> {

		public TaxiRideTSExtractor() {
			super(Time.seconds(MAX_EVENT_DELAY));
		}

		@Override
		public long extractTimestamp(TaxiRide ride) {
			if (ride.isStart) {
				return ride.startTime.getMillis();
			}
			else {
				return ride.endTime.getMillis();
			}
		}
	}

	/**
	 * Maps taxi ride to grid cell and event type.
	 * Start records use departure location, end record use arrival location.
	 */
	public static class GridCellMatcher implements MapFunction<TaxiRide, Tuple2<Integer, Boolean>> {

		@Override
		public Tuple2<Integer, Boolean> map(TaxiRide taxiRide) throws Exception {
			return new Tuple2<>(
					GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat),
					taxiRide.isStart
			);
		}
	}

	/**
	 * Counts the number of rides arriving or departing.
	 */
	public static class RideCounter implements WindowFunction<
			Tuple2<Integer, Boolean>,                // input type
			Tuple4<Integer, Long, Boolean, Integer>, // output type
			Tuple,                                   // key type
			TimeWindow>                              // window type
	{

		@SuppressWarnings("unchecked")
		@Override
		public void apply(
				Tuple key,
				TimeWindow window,
				Iterable<Tuple2<Integer, Boolean>> gridCells,
				Collector<Tuple4<Integer, Long, Boolean, Integer>> out) throws Exception {

			int cellId = ((Tuple2<Integer, Boolean>)key).f0;
			boolean isStart = ((Tuple2<Integer, Boolean>)key).f1;
			long windowTime = window.getEnd();

			int cnt = 0;
			for(Tuple2<Integer, Boolean> c : gridCells) {
				cnt += 1;
			}

			out.collect(new Tuple4<>(cellId, windowTime, isStart, cnt));
		}
	}

	/**
	 * Maps the grid cell id back to longitude and latitude coordinates.
	 */
	public static class GridToCoordinates implements
			MapFunction<Tuple4<Integer, Long, Boolean, Integer>, Tuple5<Float, Float, Long, Boolean, Integer>> {

		@Override
		public Tuple5<Float, Float, Long, Boolean, Integer> map(
				Tuple4<Integer, Long, Boolean, Integer> cellCount) throws Exception {

			return new Tuple5<>(
					GeoUtils.getGridCellCenterLon(cellCount.f0),
					GeoUtils.getGridCellCenterLat(cellCount.f0),
					cellCount.f1,
					cellCount.f2,
					cellCount.f3);
		}
	}

}
