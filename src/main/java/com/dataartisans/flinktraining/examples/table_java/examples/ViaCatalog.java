/*
 * Copyright 2017 data Artisans GmbH
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

package com.dataartisans.flinktraining.examples.table_java.examples;

import com.dataartisans.flinktraining.examples.table_java.catalog.TaxiDataCatalog;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Demonstrates the use of {@link TaxiDataCatalog} to easily register the taxi data
 * as tables under the "nyc" schema, e.g. {@code nyc.TaxiRides} and {@code nyc.TaxiFares}.
 */
public class ViaCatalog {
	public static void main(String[] args) throws Exception {

		// read parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		String ridesFile = params.getRequired("rides");
		String faresFile = params.getRequired("fares");
		int maxEventDelay = params.getInt("maxEventDelay", 60); // events are out of order by max 60 seconds
		int servingSpeedFactor = params.getInt("servingSpeedFactor", 1800); // events of 30 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// create a TableEnvironment
		StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

		// register the taxi data tables under the "nyc" schema
		tEnv.registerExternalCatalog("nyc", new TaxiDataCatalog(ridesFile, faresFile, maxEventDelay, servingSpeedFactor));

		// register user-defined functions (see FLINK-10696)
		tEnv.registerFunction("isInNYC", new GeoUtils.IsInNYC());
		tEnv.registerFunction("toCellId", new GeoUtils.ToCellId());
		tEnv.registerFunction("toCoords", new GeoUtils.ToCoords());

		// select the fares of rides within NYC
		Table results = tEnv.sqlQuery("" +
				"SELECT f.rideId, f.driverId, f.totalFare " +
				"FROM nyc.TaxiRides r INNER JOIN nyc.TaxiFares f ON r.rideId = f.rideId " +
				"WHERE isInNYC(r.startLon, r.startLat) AND isInNYC(r.endLon, r.endLat) ");

		// convert Table into an append stream and print it
		// (if instead we needed a retraction stream we would use tEnv.toRetractStream)
		tEnv.toAppendStream(results, Row.class).print();

		// execute query
		env.execute();
	}
}
