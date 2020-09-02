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

package com.ververica.flinktraining.exercises.datastream_java.utils;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class ExerciseBase {
	public static SourceFunction<TaxiRide> rides = null;
	public static SourceFunction<TaxiFare> fares = null;
	public static SourceFunction<String> strings = null;
	public static SinkFunction out = null;
	public static int parallelism = 4;

	public final static String pathToRideData = "/Users/david/stuff/flink-training-web/trainingData/nycTaxiRides.gz";
	public final static String pathToFareData = "/Users/david/stuff/flink-training-web/trainingData/nycTaxiFares.gz";

	public static SourceFunction<TaxiRide> rideSourceOrTest(SourceFunction<TaxiRide> source) {
		if (rides == null) {
			return source;
		}
		return rides;
	}

	public static SourceFunction<TaxiFare> fareSourceOrTest(SourceFunction<TaxiFare> source) {
		if (fares == null) {
			return source;
		}
		return fares;
	}

	public static SourceFunction<String> stringSourceOrTest(SourceFunction<String> source) {
		if (strings == null) {
			return source;
		}
		return strings;
	}

	public static void printOrTest(org.apache.flink.streaming.api.datastream.DataStream<?> ds) {
		if (out == null) {
			ds.print();
		} else {
			ds.addSink(out);
		}
	}

	public static void printOrTest(org.apache.flink.streaming.api.scala.DataStream<?> ds) {
		if (out == null) {
			ds.print();
		} else {
			ds.addSink(out);
		}
	}
}