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

package com.ververica.flinktraining.examples.table_java.sources;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.StreamRecordTimestamp;
import org.apache.flink.table.sources.wmstrategies.PreserveWatermarks;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.List;

/**
 * This TableSource generates a streaming table of taxi ride records which are
 * read from a gzipped input file. Each record has a time stamp and the input file must be
 * ordered by this time stamp.
 *
 * In order to simulate a realistic streaming table , the TableSource serves events proportional to
 * their timestamps. In addition, the serving of events can be delayed by a bounded random delay
 * which causes the events to be served slightly out-of-order of their timestamps.
 *
 * The serving speed of the TableSource can be adjusted by a serving speed factor.
 * A factor of 60.0 increases the logical serving time by a factor of 60, i.e., events of one
 * minute (60 seconds) are served in 1 second.
 *
 * This TableSource operates in event-time mode which must be enabled as follows:
 *
 *   StreamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
 *
 */
public class TaxiFareTableSource implements StreamTableSource<Row>, DefinedRowtimeAttributes {

	private final TaxiFareSource taxiFareSource;

	/**
	 * Serves the taxi ride rows from the specified and ordered gzipped input file.
	 * Rows are served exactly in order of their time stamps
	 * at the speed at which they were originally generated.
	 *
	 * @param dataFilePath The gzipped input file from which the taxi ride rows are read.
	 */
	public TaxiFareTableSource(String dataFilePath) {
		this.taxiFareSource = new TaxiFareSource(dataFilePath);
	}

	/**
	 * Serves the taxi ride rows from the specified and ordered gzipped input file.
	 * Rows are served exactly in order of their time stamps
	 * in a serving speed which is proportional to the specified serving speed factor.
	 *
	 * @param dataFilePath The gzipped input file from which the taxi ride rows are read.
	 * @param servingSpeedFactor The serving speed factor by which the logical serving time is adjusted.
	 */
	public TaxiFareTableSource(String dataFilePath, int servingSpeedFactor) {
		this.taxiFareSource = new TaxiFareSource(dataFilePath, 0, servingSpeedFactor);
	}

	/**
	 * Serves the taxi ride rows from the specified and ordered gzipped input file.
	 * Rows are served out-of time stamp order with specified maximum random delay
	 * in a serving speed which is proportional to the specified serving speed factor.
	 *
	 * @param dataFilePath The gzipped input file from which the taxi ride rows are read.
	 * @param maxEventDelaySecs The max time in seconds by which rows are delayed.
	 * @param servingSpeedFactor The serving speed factor by which the logical serving time is adjusted.
	 */
	public TaxiFareTableSource(String dataFilePath, int maxEventDelaySecs, int servingSpeedFactor) {
		this.taxiFareSource = new TaxiFareSource(dataFilePath, maxEventDelaySecs, servingSpeedFactor);
	}

	/**
	 * Specifies schema of the produced table.
	 *
	 * @return The schema of the produced table.
	 */
	@Override
	public TypeInformation<Row> getReturnType() {

		TypeInformation<?>[] types = new TypeInformation[] {
				Types.LONG,
				Types.LONG,
				Types.LONG,
				Types.STRING,
				Types.FLOAT,
				Types.FLOAT,
				Types.FLOAT
		};

		String[] names = new String[]{
				"rideId",
				"taxiId",
				"driverId",
				"paymentType",
				"tip",
				"tolls",
				"totalFare"
		};

		return new RowTypeInfo(types, names);
	}

	@Override
	public TableSchema getTableSchema() {
		TypeInformation<?>[] types = new TypeInformation[] {
				Types.LONG,
				Types.LONG,
				Types.LONG,
				Types.STRING,
				Types.FLOAT,
				Types.FLOAT,
				Types.FLOAT,
				Types.SQL_TIMESTAMP
		};

		String[] names = new String[]{
				"rideId",
				"taxiId",
				"driverId",
				"paymentType",
				"tip",
				"tolls",
				"totalFare",
				"eventTime"
		};

		return new TableSchema(names, types);
	}

	@Override
	public String explainSource() {
		return "TaxiFares";
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {

		return execEnv
			.addSource(this.taxiFareSource)
			.map(new TaxiFareToRow()).returns(getReturnType());
	}

	@Override
	public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
		RowtimeAttributeDescriptor descriptor = new RowtimeAttributeDescriptor("eventTime", new StreamRecordTimestamp(), new PreserveWatermarks());
		return Collections.singletonList(descriptor);
	}

	/**
	 * Converts TaxiRide records into table Rows.
	 */
	public static class TaxiFareToRow implements MapFunction<TaxiFare, Row> {

		@Override
		public Row map(TaxiFare fare) throws Exception {

			return Row.of(
					fare.rideId,
					fare.taxiId,
					fare.driverId,
					fare.paymentType,
					fare.tip,
					fare.tolls,
					fare.totalFare);
		}
	}

}
