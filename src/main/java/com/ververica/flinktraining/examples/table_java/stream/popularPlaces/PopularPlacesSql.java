/*
 * Copyright 2017 data Artisans GmbH, 2019 Ververica GmbH
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

package com.ververica.flinktraining.examples.table_java.stream.popularPlaces;

import com.ververica.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.ververica.flinktraining.examples.table_java.sources.TaxiRideTableSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class PopularPlacesSql {

	public static void main(String[] args) throws Exception {

		// read parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		String input = params.getRequired("input");

		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// create a TableEnvironment
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		// register TaxiRideTableSource as table "TaxiRides"
		tEnv.registerTableSource(
				"TaxiRides",
				new TaxiRideTableSource(
						input,
						maxEventDelay,
						servingSpeedFactor));

		// register user-defined functions
		tEnv.registerFunction("isInNYC", new GeoUtils.IsInNYC());
		tEnv.registerFunction("toCellId", new GeoUtils.ToCellId());
		tEnv.registerFunction("toCoords", new GeoUtils.ToCoords());

		Table results = tEnv.sqlQuery(
			"SELECT " +
				"toCoords(cell), wstart, wend, isStart, popCnt " +
			"FROM " +
				"(SELECT " +
					"cell, " +
					"isStart, " +
					"HOP_START(eventTime, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE) AS wstart, " +
					"HOP_END(eventTime, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE) AS wend, " +
					"COUNT(isStart) AS popCnt " +
				"FROM " +
					"(SELECT " +
						"eventTime, " +
						"isStart, " +
						"CASE WHEN isStart THEN toCellId(startLon, startLat) ELSE toCellId(endLon, endLat) END AS cell " +
					"FROM TaxiRides " +
					"WHERE isInNYC(startLon, startLat) AND isInNYC(endLon, endLat)) " +
				"GROUP BY cell, isStart, HOP(eventTime, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE)) " +
			"WHERE popCnt > 20"
			);

		// convert Table into an append stream and print it
		// (if instead we needed a retraction stream we would use tEnv.toRetractStream)
		tEnv.toAppendStream(results, Row.class).print();

		// execute query
		env.execute();
	}

}
