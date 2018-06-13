package com.dataartisans.flinktraining.exercises.datastream_java.utils;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
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

	public static void printOrTest(org.apache.flink.streaming.api.datastream.DataStream<?> ds) {
		if (out == null) {
			ds.print();
		}
		ds.addSink(out);
	}

	public static void printOrTest(org.apache.flink.streaming.api.scala.DataStream<?> ds) {
		if (out == null) {
			ds.print();
		}
		ds.addSink(out);
	}
}