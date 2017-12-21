package com.dataartisans.flinktraining.exercises.datastream_java.process;

import com.dataartisans.flinktraining.exercises.datastream_java.basics.RideCleansing;
import com.dataartisans.flinktraining.exercises.datastream_java.basics.RideCleansingSolution;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;
import org.mortbay.log.Log;

public class LongRidesSolution extends ExerciseBase {
	public static void main(String[] args) throws Exception {
		final String pathToRideData = "/Users/david/stuff/flink-training/trainingData/nycTaxiRides.gz";

		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", pathToRideData);

		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(parallelism);

		// start the data generator
		DataStream<TaxiRide> rides = env.addSource(sourceOrTest(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor)));

		DataStream<TaxiRide> longRides = rides
				.filter(new RideCleansingSolution.NYCFilter())
				.keyBy("rideId")
				.process(new LongRides.MatchFunction());

		longRides.addSink(printOrTest(new PrintSinkFunction<>()));

		env.execute("Long Taxi Rides");
	}

	private static class MatchFunction extends ProcessFunction<TaxiRide, TaxiRide> {
		// keyed, managed state
		// holds an END event if the ride has ended, otherwise a START event
		private ValueState<TaxiRide> rideState;

		@Override
		public void open(Configuration config) {
			ValueStateDescriptor<TaxiRide> startDescriptor =
					new ValueStateDescriptor<>("saved ride", TaxiRide.class);
			rideState = getRuntimeContext().getState(startDescriptor);
		}

		@Override
		public void processElement(TaxiRide ride, Context context, Collector<TaxiRide> out) throws Exception {
			TimerService timerService = context.timerService();

			if (ride.isStart) {
				// the matching END might have arrived first; don't overwrite it
				if (rideState.value() == null) {
					rideState.update(ride);
				}
			} else {
				rideState.update(ride);
			}

			timerService.registerEventTimeTimer(ride.getEventTime() + 120 * 60 * 1000);
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext context, Collector<TaxiRide> out) throws Exception {
			TaxiRide savedRide = rideState.value();
			if (savedRide != null && savedRide.isStart) {
				out.collect(savedRide);
			}
			rideState.clear();
		}
	}

}
