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
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Java reference implementation for the "Popular Places" exercise of the Flink training
 * (http://training.data-artisans.com).
 *
 * The task of the exercise is to identify every five minutes popular areas where many taxi rides
 * arrived or departed in the last 15 minutes.
 * The results are written into an Elasticsearch index.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
public class PopularPlacesToES {

	public static void main(String[] args) throws Exception {

		// read parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		String input = params.getRequired("input");

		final int popThreshold = 20; // threshold for popular places
		final int maxEventDelay = 60; // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// start the data generator
		DataStream<TaxiRide> rides = env.addSource(
				new TaxiRideSource(input, maxEventDelay, servingSpeedFactor));

		// find popular places
		DataStream<Tuple5<Float, Float, Long, Boolean, Integer>> popularPlaces = rides
				// remove all rides which are not within NYC
				.filter(new NYCFilter())
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

		Map<String, String> config = new HashMap<>();
		// This instructs the sink to emit after every element, otherwise they would be buffered
		config.put("bulk.flush.max.actions", "10");
		config.put("cluster.name", "elasticsearch");

		List<InetSocketAddress> transports = new ArrayList<>();
		transports.add(new InetSocketAddress(InetAddress.getByName("localhost"), 9300));

		popularPlaces.addSink(new ElasticsearchSink<>(
				config,
				transports,
				new PopularPlaceInserter()));

		// execute the transformation pipeline
		env.execute("Popular Places to Elasticsearch");
	}

	/**
	 * Inserts popular places into the "nyc-places" index.
	 */
	public static class PopularPlaceInserter
			implements ElasticsearchSinkFunction<Tuple5<Float, Float, Long, Boolean, Integer>> {

		// construct index request
		@Override
		public void process(
				Tuple5<Float, Float, Long, Boolean, Integer> record,
				RuntimeContext ctx,
				RequestIndexer indexer) {

			// construct JSON document to index
			Map<String, String> json = new HashMap<>();
			json.put("time", record.f2.toString());         // timestamp
			json.put("location", record.f1+","+record.f0);  // lat,lon pair
			json.put("isStart", record.f3.toString());      // isStart
			json.put("cnt", record.f4.toString());          // count

			IndexRequest rqst = Requests.indexRequest()
					.index("nyc-places")        // index name
					.type("popular-locations")  // mapping name
					.source(json);

			indexer.add(rqst);
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

	public static class NYCFilter implements FilterFunction<TaxiRide> {
		@Override
		public boolean filter(TaxiRide taxiRide) throws Exception {

			return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
					GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
		}
	}

}
