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

package com.dataartisans.flinktraining.exercises.datastream_java.utils.influxdb;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.concurrent.TimeUnit;

public class InfluxDBSink<T extends DataPoint<? extends Number>> extends RichSinkFunction<T> {

	private final ParameterTool parameters;
	private transient InfluxDB influxDB = null;
	private final static String DEFAULT_DATABASE_NAME = "sineWave";
	private final static String DEFAULT_FIELD_NAME = "value";
	private String measurement;
	private String fieldName;

	public InfluxDBSink(String measurement, ParameterTool parameters){
		this.measurement = measurement;
		this.parameters = parameters;
	}

	@Override
	public void open(Configuration unused) throws Exception {
		super.open(unused);
		influxDB = InfluxDBFactory.connect(parameters.get("url", "http://localhost:8086"),
				parameters.get("user", "admin"),
				parameters.get("password", "admin"));
		influxDB.createDatabase(parameters.get("db", DEFAULT_DATABASE_NAME));
		influxDB.enableBatch(2000, 100, TimeUnit.MILLISECONDS);
		this.fieldName = parameters.get("field", DEFAULT_FIELD_NAME);
	}

	@Override
	public void close() throws Exception {
		super.close();
	}

	@Override
	public void invoke(T dataPoint, SinkFunction.Context context) throws Exception {
		Point.Builder builder = Point.measurement(measurement)
				.time(dataPoint.getTimeStampMs(), TimeUnit.MILLISECONDS)
				.addField(this.fieldName, dataPoint.getValue());

		if(dataPoint instanceof KeyedDataPoint){
			builder.tag("key", ((KeyedDataPoint) dataPoint).getKey());
		}

		Point p = builder.build();

		influxDB.write(DEFAULT_DATABASE_NAME, "autogen", p);
	}
}