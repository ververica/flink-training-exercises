/*
 * Copyright 2015 data Artisans GmbH, 2019 Ververica GmbH
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

package com.ververica.flinktraining.examples.datastream_java.state;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.CheckpointedTaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.ververica.flinktraining.exercises.datastream_java.utils.TravelTimePredictionModel;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * Java reference implementation for the "Travel Time Prediction" exercise of the Flink training
 * (http://training.ververica.com).
 *
 * The task of the exercise is to continuously train a regression model that predicts
 * the travel time of a taxi based on the information of taxi ride end events.
 * For taxi ride start events, the model should be queried to estimate its travel time.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
public class TravelTimePrediction {

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.getRequired("input");

		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// operate in Event-time
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// create a checkpoint every 5 seconds
		env.enableCheckpointing(5000);
		// try to restart 60 times with 10 seconds delay (10 Minutes)
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(60, Time.of(10, TimeUnit.SECONDS)));

		// start the data generator
		DataStream<TaxiRide> rides = env.addSource(
				new CheckpointedTaxiRideSource(input, servingSpeedFactor));

		DataStream<Tuple2<Long, Integer>> predictions = rides
			// filter out rides that do not start or stop in NYC
			.filter(new NYCFilter())
			// map taxi ride events to the grid cell of the destination
			.map(new GridCellMatcher())
			// organize stream by destination
			.keyBy(0)
			// predict and refine model per destination
			.flatMap(new PredictionModel());

		// print the predictions
		predictions.print();

		// run the prediction pipeline
		env.execute("Taxi Ride Prediction");
	}

	public static class NYCFilter implements FilterFunction<TaxiRide> {

		@Override
		public boolean filter(TaxiRide taxiRide) throws Exception {

			return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
					GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
		}
	}

	/**
	 * Maps the taxi ride event to the grid cell of the destination location.
	 */
	public static class GridCellMatcher implements MapFunction<TaxiRide, Tuple2<Integer, TaxiRide>> {

		@Override
		public Tuple2<Integer, TaxiRide> map(TaxiRide ride) throws Exception {
			int endCell = GeoUtils.mapToGridCell(ride.endLon, ride.endLat);

			return new Tuple2<>(endCell, ride);
		}
	}

	/**
	 * Predicts the travel time for taxi ride start events based on distance and direction.
	 * Incrementally trains a regression model using taxi ride end events.
	 */
	public static class PredictionModel extends RichFlatMapFunction<Tuple2<Integer, TaxiRide>, Tuple2<Long, Integer>> {

		private transient ValueState<TravelTimePredictionModel> modelState;

		@Override
		public void flatMap(Tuple2<Integer, TaxiRide> val, Collector<Tuple2<Long, Integer>> out) throws Exception {

			// fetch operator state
			TravelTimePredictionModel model = modelState.value();
			if (model == null) {
				model = new TravelTimePredictionModel();
			}

			TaxiRide ride = val.f1;
			// compute distance and direction
			double distance = GeoUtils.getEuclideanDistance(ride.startLon, ride.startLat, ride.endLon, ride.endLat);
			int direction = GeoUtils.getDirectionAngle(ride.endLon, ride.endLat, ride.startLon, ride.startLat);

			if (ride.isStart) {
				// we have a start event: Predict travel time
				int predictedTime = model.predictTravelTime(direction, distance);
				// emit prediction
				out.collect(new Tuple2<>(ride.rideId, predictedTime));
			} else {
				// we have an end event: Update model
				// compute travel time in minutes
				double travelTime = (ride.endTime.getMillis() - ride.startTime.getMillis()) / 60000.0;
				// refine model
				model.refineModel(direction, distance, travelTime);
				// update operator state
				modelState.update(model);
			}
		}

		@Override
		public void open(Configuration config) {
			// obtain key-value state for prediction model
			ValueStateDescriptor<TravelTimePredictionModel> descriptor =
					new ValueStateDescriptor<>(
							// state name
							"regressionModel",
							// type information of state
							TypeInformation.of(TravelTimePredictionModel.class));
			modelState = getRuntimeContext().getState(descriptor);
		}
	}

}
