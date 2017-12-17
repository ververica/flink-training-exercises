package com.dataartisans.flinktraining.exercises.datastream_java.utils;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;
import static org.junit.Assert.assertEquals;

public abstract class TaxiRideExerciseBase<OUT> implements TrainingExerciseBase<TaxiRide, OUT> {
	private static String pathToRideData = "/Users/david/stuff/flink-training/trainingData/nycTaxiRides.gz";
	public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

	public DataStream<TaxiRide> realStream(String[] args) {
		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", pathToRideData);

		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// start the data generator
		DataStream<TaxiRide> rides = env.addSource(
				new TaxiRideSource(input, maxEventDelay, servingSpeedFactor));

		return rides;
	}

	public void run(DataStream<TaxiRide> rides) throws Exception {
		DataStream<OUT> result = perform(rides);
		result.print();
		env.execute();
	}

	public void test(DataStream<TaxiRide> rides) throws Exception {
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);
		CollectSink.values.clear();

		DataStream<OUT> result = perform(rides);
		result.addSink(new CollectSink<OUT>());
		env.execute();

		assertEquals(expectedResult(), CollectSink.values);
	}

	// create a testing sink
	private static class CollectSink<OUT> implements SinkFunction<OUT> {

		// must be static
		public static final List values = new ArrayList<>();

		@Override
		public synchronized void invoke(OUT value) throws Exception {
			values.add(value);
		}
	}
}
