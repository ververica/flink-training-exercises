package com.dataartisans.flinktraining.exercises.datastream_java.utils;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class ExerciseBase {
	public static SourceFunction<TaxiRide> in = null;
	public static SinkFunction out = null;
	public static int parallelism = 4;

	public static SourceFunction<TaxiRide> sourceOrTest(SourceFunction<TaxiRide> source) {
		if (in == null) {
			return source;
		}
		return in;
	}

	public static SinkFunction printOrTest(SinkFunction sink) {
		if (out == null) {
			return sink;
		}
		return out;
	}
}