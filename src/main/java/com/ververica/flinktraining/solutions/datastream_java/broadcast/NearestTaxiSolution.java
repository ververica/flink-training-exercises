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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.Random;

/**
 * Java reference implementation for the "Nearest Future Taxi" exercise of the Flink training
 * (http://training.ververica.com). This solution doesn't worry about leaking state.
 *
 * Given a location that is broadcast, the goal of this exercise is to watch the stream of
 * taxi rides, and report on the taxis that complete their ride closest to the requested location.
 *
 * Parameters:
 * -input path-to-input-file
 *
 * Use
 *
 *     nc -lk 9999
 *
 * (or nc -l -p 9999, depending on your version of netcat)
 * to establish a socket stream from stdin on port 9999.
 * On Windows you can use ncat from https://nmap.org/ncat/.
 *
 * Some good locations:
 *
 * -74, 41 					(Near, but outside the city to the NNW)
 * -73.7781, 40.6413 		(JFK Airport)
 * -73.977664, 40.761484	(Museum of Modern Art)
 */
public class NearestTaxiSolution extends ExerciseBase {

	private static class Query {

		private final long queryId;
		private final float longitude;
		private final float latitude;

		Query(final float longitude, final float latitude) {
			this.queryId = new Random().nextLong();
			this.longitude = longitude;
			this.latitude = latitude;
		}

		Long getQueryId() {
			return queryId;
		}

		public float getLongitude() {
			return longitude;
		}

		public float getLatitude() {
			return latitude;
		}

		@Override
		public String toString() {
			return "Query{" +
					"id=" + queryId +
					", longitude=" + longitude +
					", latitude=" + latitude +
					'}';
		}
	}

	final static MapStateDescriptor queryDescriptor = new MapStateDescriptor<>(
			"queries",
			BasicTypeInfo.LONG_TYPE_INFO,
			TypeInformation.of(Query.class));

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToRideData);

		final int maxEventDelay = 60;       	// events are out of order by at most 60 seconds
		final int servingSpeedFactor = 600; 	// 10 minutes worth of events are served every second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor));

		// add a socket source
		BroadcastStream<Query> queryStream = env.socketTextStream("localhost", 9999)
				.map(new MapFunction<String, Query>() {
					@Override
					public Query map(String msg) throws Exception {
						String[] parts = msg.split(",\\s*");
						return new Query(
								Float.valueOf(parts[0]),	// longitude
								Float.valueOf(parts[1]));	// latitude
					}
				})
				.broadcast(queryDescriptor);

		DataStream<Tuple3<Long, Long, Float>> reports = rides
				.keyBy((TaxiRide ride) -> ride.taxiId)
				.connect(queryStream)
				.process(new QueryFunction());

		DataStream<Tuple3<Long, Long, Float>> nearest = reports
				// key by the queryId
				.keyBy(x -> x.f0)
				// the minimum, for each query, by distance
				.minBy(2);

		nearest.print();

		env.execute("Nearest Available Taxi");
	}

	// Note that in order to have consistent results after a restore from a checkpoint, the
	// behavior of this method must be deterministic, and NOT depend on characteristics of an
	// individual sub-task.
	public static class QueryFunction extends KeyedBroadcastProcessFunction<Long, TaxiRide, Query, Tuple3<Long, Long, Float>> {

		@Override
		public void processBroadcastElement(Query query, Context ctx, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
			System.out.println("new query " + query);
			ctx.getBroadcastState(queryDescriptor).put(query.getQueryId(), query);
		}

		@Override
		// Output (queryId, taxiId, euclidean distance) for every query, if the taxi ride is now ending.
		public void processElement(TaxiRide ride, ReadOnlyContext ctx, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
			if (!ride.isStart) {
				Iterable<Map.Entry<Long, Query>> entries = ctx.getBroadcastState(queryDescriptor).immutableEntries();

				for (Map.Entry<Long, Query> entry: entries) {
					final Query query = entry.getValue();
					final float kilometersAway = (float) ride.getEuclideanDistance(query.getLongitude(), query.getLatitude());

					out.collect(new Tuple3<>(
							query.getQueryId(),
							ride.taxiId,
							kilometersAway
					));
				}
			}
		}
	}
}