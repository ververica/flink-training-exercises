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

package com.ververica.flinktraining.exercises.datastream_java.utils;

import org.apache.commons.math3.stat.regression.SimpleRegression;

/**
 * TravelTimePredictionModel provides a very simple regression model to predict the travel time
 * to a destination location depending on the direction and distance of the departure location.
 *
 * The model builds for multiple direction intervals (think of it as north, north-east, east, etc.)
 * a linear regression model (Apache Commons Math, SimpleRegression) to predict the travel time based
 * on the distance.
 *
 * NOTE: This model is not mean for accurate predictions but rather to illustrate Flink's handling
 * of operator state.
 *
 */
public class TravelTimePredictionModel {

	private static int NUM_DIRECTION_BUCKETS = 8;
	private static int BUCKET_ANGLE = 360 / NUM_DIRECTION_BUCKETS;

	SimpleRegression[] models;

	public TravelTimePredictionModel() {
		models = new SimpleRegression[NUM_DIRECTION_BUCKETS];
		for (int i = 0; i < NUM_DIRECTION_BUCKETS; i++) {
			models[i] = new SimpleRegression(false);
		}
	}

	/**
	 * Predicts the time of a taxi to arrive from a certain direction and Euclidean distance.
	 *
	 * @param direction The direction from which the taxi arrives.
	 * @param distance The Euclidean distance that the taxi has to drive.
	 * @return A prediction of the time that the taxi will be traveling or -1 if no prediction is
	 *         possible, yet.
	 */
	public int predictTravelTime(int direction, double distance) {
		byte directionBucket = getDirectionBucket(direction);
		double prediction = models[directionBucket].predict(distance);

		if (Double.isNaN(prediction)) {
			return -1;
		}
		else {
			return (int)prediction;
		}
	}

	/**
	 * Refines the travel time prediction model by adding a data point.
	 *
	 * @param direction The direction from which the taxi arrived.
	 * @param distance The Euclidean distance that the taxi traveled.
	 * @param travelTime The actual travel time of the taxi.
	 */
	public void refineModel(int direction, double distance, double travelTime) {
		byte directionBucket = getDirectionBucket(direction);
		models[directionBucket].addData(distance, travelTime);
	}

	/**
	 * Converts a direction angle (degrees) into a bucket number.
	 *
	 * @param direction An angle in degrees.
	 * @return A direction bucket number.
	 */
	private byte getDirectionBucket(int direction) {
		return (byte)(direction / BUCKET_ANGLE);
	}

}
