package com.dataartisans.flinktraining.exercises.datastream_java.testing;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.MissingSolutionException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.*;

public abstract class TaxiRideTestBase<OUT> {
	public static class TestRideSource implements SourceFunction<TaxiRide> {
		private volatile boolean running = true;
		private Object[] testStream;

		public TestRideSource(Object ... ridesOrWatermarks) {
			this.testStream = ridesOrWatermarks;
		}

		@Override
		public void run(SourceContext<TaxiRide> ctx) throws Exception {
			for (int i = 0; (i < testStream.length) && running; i++) {
				if (testStream[i] instanceof TaxiRide) {
					TaxiRide ride = (TaxiRide) testStream[i];
					ctx.collectWithTimestamp(ride, ride.getEventTime());
				} else if (testStream[i] instanceof Long) {
					Long ts = (Long) testStream[i];
					ctx.emitWatermark(new Watermark(ts));
				} else {
					throw new RuntimeException(testStream[i].toString());
				}
			}
			// source is finite, so it will have an implicit MAX watermark when it finishes
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	public static class TestFareSource implements SourceFunction<TaxiFare> {
		private volatile boolean running = true;
		private Object[] testStream;

		public TestFareSource(Object ... faresOrWatermarks) {
			this.testStream = faresOrWatermarks;
		}

		@Override
		public void run(SourceContext<TaxiFare> ctx) throws Exception {
			for (int i = 0; (i < testStream.length) && running; i++) {
				if (testStream[i] instanceof TaxiFare) {
					TaxiFare fare = (TaxiFare) testStream[i];
					ctx.collectWithTimestamp(fare, fare.getEventTime());
				} else if (testStream[i] instanceof Long) {
					Long ts = (Long) testStream[i];
					ctx.emitWatermark(new Watermark(ts));
				} else {
					throw new RuntimeException(testStream[i].toString());
				}
			}
			// source is finite, so it will have an implicit MAX watermark when it finishes
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	public static class TestSink<OUT> implements SinkFunction<OUT> {

		// must be static
		public static final List values = new ArrayList<>();

		@Override
		public void invoke(OUT value, Context context) throws Exception {
			values.add(value);
		}
	}

	public interface Testable {
		public abstract void main() throws Exception;
	}

	public static Testable missingExercise = () -> MissingExercise.main(new String[]{});

	protected List<OUT> runTest(TestRideSource source, TestSink<OUT> sink, Testable exercise, Testable solution) throws Exception {
		sink.values.clear();
		ExerciseBase.rides = source;
		ExerciseBase.out = sink;
		ExerciseBase.parallelism = 1;

		return execute(sink, exercise, solution);
	}

	protected List<OUT> runTest(TestFareSource source, TestSink<OUT> sink, Testable exercise, Testable solution) throws Exception {
		sink.values.clear();
		ExerciseBase.fares = source;
		ExerciseBase.out = sink;
		ExerciseBase.parallelism = 1;

		return execute(sink, exercise, solution);
	}

	protected List<OUT> runTest(TestRideSource rides, TestFareSource fares, TestSink<OUT> sink, Testable exercise, Testable solution) throws Exception {
		sink.values.clear();
		ExerciseBase.rides = rides;
		ExerciseBase.fares = fares;
		ExerciseBase.out = sink;
		ExerciseBase.parallelism = 1;

		return execute(sink, exercise, solution);
	}

	private List<OUT> execute(TestSink<OUT> sink, Testable exercise, Testable solution) throws Exception {
		try {
			exercise.main();
		} catch (JobExecutionException | MissingSolutionException e) {
			if (e instanceof MissingSolutionException ||
					(e.getCause() != null && e.getCause() instanceof MissingSolutionException)) {
				solution.main();
			} else {
				throw e;
			}
		}

		return sink.values;
	}
}
