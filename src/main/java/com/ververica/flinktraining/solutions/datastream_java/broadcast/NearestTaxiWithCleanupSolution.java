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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;

/**
 * Java reference implementation for the "Nearest Future Taxi" exercise of the Flink training
 * (http://training.ververica.com). This solution takes care not to leak state.
 *
 * Given a location that is broadcast, the goal of this exercise is to watch the stream of
 * taxi rides, and report on the taxis that complete their ride closest to the requested location.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
public class NearestTaxiWithCleanupSolution extends ExerciseBase {

	final static long MIN_QUERY_RETENTION_TIME = 15 * 60 * 1000;			// 15 minutes
	final static long CLOSEST_REPORT_RETENTION_TIME = 8 * 60 * 60 * 1000;	// 8 hours

	private static class Query {

		private final long queryId;
		private final float longitude;
		private final float latitude;
		private long timestamp;

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

		public Query setTimestamp(long t) {
			this.timestamp = t;
			return this;
		}

		public long getTimestamp() {
			return timestamp;
		}

		@Override
		public String toString() {
			return "Query{" +
					"id=" + queryId +
					", longitude=" + longitude +
					", latitude=" + latitude +
					", timestamp=" + timestamp +
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

		DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor)));

		// add a socket source
		BroadcastStream<Query> queryStream = env.socketTextStream("localhost", 9999)
				.assignTimestampsAndWatermarks(new QueryStreamAssigner())
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
				.keyBy(new KeySelector<Tuple3<Long, Long, Float>, Long>() {
					@Override
					public Long getKey(Tuple3<Long, Long, Float> value) throws Exception {
						return value.f0;
					}
				})
				.process(new ClosestTaxi());

		printOrTest(nearest);

		env.execute("Nearest Available Taxi");
	}

	// Once the two streams are connected, the Watermark of the KeyedBroadcastProcessFunction operator
	// will be the minimum of the Watermarks of the two connected streams. Our query stream has a default
	// Watermark at Long.MIN_VALUE, and this will hold back the event time clock of the
	// KeyedBroadcastProcessFunction, unless we do something about it.
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

	// Only pass thru values that are new minima -- remove duplicates.
	// Clear state for a given query once there has been no new minima
	// for CLOSEST_REPORT_RETENTION_TIME.
	public static class ClosestTaxi extends KeyedProcessFunction<Long, Tuple3<Long, Long, Float>, Tuple3<Long, Long, Float>> {
		private transient ValueState<Tuple2<Long, Float>> closest;
		private transient ValueState<Long> latestTimer;

		@Override
		public void open(Configuration parameters) throws Exception {
			ValueStateDescriptor<Tuple2<Long, Float>> descriptor =
					new ValueStateDescriptor<Tuple2<Long, Float>>(
							// state name
							"report",
							// type information of state
							TypeInformation.of(new TypeHint<Tuple2<Long, Float>>() {}));
			closest = getRuntimeContext().getState(descriptor);
			latestTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("latest timer", Long.class));
		}

		@Override
		public void processElement(Tuple3<Long, Long, Float> report, Context ctx, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
			if (closest.value() == null || report.f2 < closest.value().f1) {
				long cleanUpTime = ctx.timestamp() + CLOSEST_REPORT_RETENTION_TIME;
				ctx.timerService().registerEventTimeTimer(cleanUpTime);
				latestTimer.update(cleanUpTime);
				closest.update(new Tuple2<>(report.f1, report.f2));
				out.collect(report);
			}
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
			// only clean up state if this timer is the latest timer for this key
			if (timestamp == latestTimer.value()) {
				System.out.println("clearing state for " + ctx.getCurrentKey());
				latestTimer.clear();
				closest.clear();
			}
		}
	}

	// Note that in order to have consistent results after a restore from a checkpoint, the
	// behavior of this method must be deterministic, and NOT depend on characterisitcs of an
	// individual sub-task.
	public static class QueryFunction extends KeyedBroadcastProcessFunction<Long, TaxiRide, Query, Tuple3<Long, Long, Float>> {

		@Override
		public void open(Configuration config) {
		}

		@Override
		public void processBroadcastElement(Query query, Context ctx, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
			long wm = ctx.currentWatermark();
			query.setTimestamp(wm);

			// garbage collect older queries
			// note that we cannot associate timers with broadcast state (because timers are always keyed)
			Iterator<Map.Entry<Long, Query>> queries = ctx.getBroadcastState(queryDescriptor).iterator();
			queries.forEachRemaining(new Consumer<Map.Entry<Long, Query>>() {
				@Override
				public void accept(Map.Entry<Long, Query> entry) {
					final Query query = entry.getValue();
					if (wm - query.getTimestamp() > MIN_QUERY_RETENTION_TIME) {
						System.out.println("removing query " + query);
						queries.remove();
					}
				}
			});

			if (query.getLongitude() != 0) {
				System.out.println("new query " + query);
				ctx.getBroadcastState(queryDescriptor).put(query.getQueryId(), query);
			}
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