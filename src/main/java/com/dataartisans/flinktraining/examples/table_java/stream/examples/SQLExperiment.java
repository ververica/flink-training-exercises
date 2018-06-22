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

package com.dataartisans.flinktraining.examples.table_java.stream.examples;

import com.dataartisans.flinktraining.examples.table_java.sources.TaxiFareTableSource;
import com.dataartisans.flinktraining.examples.table_java.sources.TaxiRideTableSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class SQLExperiment {

	public static void main(String[] args) throws Exception {

		// read parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		String ridesFile = params.getRequired("rides");
		String faresFile = params.getRequired("fares");

		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// create a TableEnvironment
		StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

		tEnv.registerTableSource("TaxiRides", new TaxiRideTableSource(ridesFile, maxEventDelay, servingSpeedFactor));
		tEnv.registerTableSource("TaxiFares", new TaxiFareTableSource(faresFile, maxEventDelay, servingSpeedFactor));

		Table results = tEnv.sqlQuery(
				"SELECT TUMBLE_END(eventTime, INTERVAL '1' HOUR) AS wend, driverId, SUM(tip) as tips " +
						"FROM TaxiFares " +
						"GROUP BY driverId, TUMBLE(eventTime, INTERVAL '1' HOUR) " +
						"HAVING SUM(tip) > 50"
		);

		// convert Table into an append stream and print it
		tEnv.toRetractStream(results, Row.class).print();

		// execute query
		env.execute();
	}

}
