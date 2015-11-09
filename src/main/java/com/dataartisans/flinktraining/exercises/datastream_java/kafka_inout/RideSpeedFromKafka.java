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

package com.dataartisans.flinktraining.exercises.datastream_java.kafka_inout;

import com.dataartisans.flinktraining.exercises.datastream_java.utils.TaxiRideSchema;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * Java reference implementation for the "Ride Speed" exercise of the Flink training
 * (http://dataartisans.github.io/flink-training).
 *
 * The task of the exercise is to read taxi ride records from an Apache Kafka topic and compute
 * the average speed of completed taxi rides.
 *
 */
public class RideSpeedFromKafka {

	private static final String LOCAL_ZOOKEEPER_HOST = "localhost:2181";
	private static final String LOCAL_KAFKA_BROKER = "localhost:9092";
	private static final String RIDE_SPEED_GROUP = "rideSpeedGroup";

	public static void main(String[] args) throws Exception {

		final int maxEventDelay = 60; // events are out of order by max 60 seconds

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// configure the Kafka consumer
		Properties kafkaProps = new Properties();
		kafkaProps.setProperty("zookeeper.connect", LOCAL_ZOOKEEPER_HOST);
		kafkaProps.setProperty("bootstrap.servers", LOCAL_KAFKA_BROKER);
		kafkaProps.setProperty("group.id", RIDE_SPEED_GROUP);

		// create a TaxiRide data stream
		DataStream<TaxiRide> rides = env
				.addSource(new FlinkKafkaConsumer082<TaxiRide>(
						RideCleansingToKafka.CLEANSED_RIDES_TOPIC,
						new TaxiRideSchema(),
						kafkaProps)
				)
				.assignTimestamps(new TimestampExtractor<TaxiRide>() {

					long curWatermark;

					@Override
					public long extractTimestamp(TaxiRide ride, long currentTimestamp) {
						return ride.time.getMillis();
					}

					@Override
					public long extractWatermark(TaxiRide ride, long currentTimestamp) {
						curWatermark = currentTimestamp - (maxEventDelay * 1000);
						return -1;
					}

					@Override
					public long getCurrentWatermark() {
						return curWatermark;
					}
				});

		DataStream<Tuple2<Long, Float>> rideSpeeds = rides
				// group records by rideId
				.keyBy("rideId")
				// compute the average speed of a ride
				.flatMap(new SpeedComputer());

		// emit the result on stdout
		rideSpeeds.print();

		// run the transformation pipeline
		env.execute("Average Ride Speed");
	}


	/**
	 * Computes the average speed of a taxi ride
	 *
	 */
	public static class SpeedComputer extends RichFlatMapFunction<TaxiRide, Tuple2<Long, Float>> {

		private OperatorState<TaxiRide> state;

		@Override
		public void open(Configuration parameters) throws Exception {
			state = this.getRuntimeContext().getKeyValueState("ride", TaxiRide.class, null);
		}

		@Override
		public void flatMap(TaxiRide taxiRide, Collector<Tuple2<Long, Float>> out)
				throws Exception {

			if(state.value() == null) {
				// we received the first element. Put it into the state
				state.update(taxiRide);
			}
			else {
				// we received the second element. Compute the speed.
				TaxiRide startEvent = taxiRide.isStart ? taxiRide : state.value();
				TaxiRide endEvent = taxiRide.isStart ? state.value() : taxiRide;

				long timeDiff = endEvent.time.getMillis() - startEvent.time.getMillis();
				float avgSpeed;

				if(timeDiff != 0) {
					// speed = distance / time
					avgSpeed = (endEvent.travelDistance / timeDiff) * (1000 * 60 * 60);
				}
				else {
					avgSpeed = -1f;
				}
				// emit average speed
				out.collect(new Tuple2<Long, Float>(startEvent.rideId, avgSpeed));

				// clear state to free memory
				state.update(null);
			}
		}
	}

}
