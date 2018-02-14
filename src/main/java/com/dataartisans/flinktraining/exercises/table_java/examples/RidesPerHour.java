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

package com.dataartisans.flinktraining.exercises.table_java.examples;

import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.dataartisans.flinktraining.exercises.table_java.sources.TaxiRideTableSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class RidesPerHour {
	public static void main(String[] args) throws Exception {

		// read parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		String input = params.getRequired("input");

		final int maxEventDelay = 60;       	// events are out of order by max 60 seconds
		final int servingSpeedFactor = 1800; 	// events of 30 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// create a TableEnvironment
		StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

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
				//"SELECT TUMBLE_START(eventTime, INTERVAL '1' HOUR), isStart, count(isStart) FROM TaxiRides GROUP BY isStart, TUMBLE(eventTime, INTERVAL '1' HOUR)"
				//"SELECT avg(endTime - startTime), passengerCnt FROM TaxiRides GROUP BY passengerCnt"
				"SELECT CAST (toCellId(endLon, endLat) AS VARCHAR), eventTime," +
				"COUNT(*) OVER (" +
					"PARTITION BY toCellId(endLon, endLat) ORDER BY eventTime RANGE BETWEEN INTERVAL '10' MINUTE PRECEDING AND CURRENT ROW" +
				") " +
				"FROM( SELECT * FROM TaxiRides WHERE not isStart AND toCellId(endLon, endLat) = 50801 )"
		);

		// convert Table into an append stream and print it
		// (if instead we needed a retraction stream we would use tEnv.toRetractStream)
		tEnv.toRetractStream(results, Row.class).print();

		// execute query
		env.execute();
	}
}
