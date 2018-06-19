/*
 * Copyright 2018 data Artisans GmbH
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

	protected List<OUT> runApp(TestRideSource source, TestSink<OUT> sink, Testable exercise, Testable solution) throws Exception {
		ExerciseBase.rides = source;

		return execute(sink, exercise, solution);
	}

	protected List<OUT> runApp(TestFareSource source, TestSink<OUT> sink, Testable exercise, Testable solution) throws Exception {
		ExerciseBase.fares = source;

		return execute(sink, exercise, solution);
	}

	protected List<OUT> runApp(TestRideSource rides, TestFareSource fares, TestSink<OUT> sink, Testable exercise, Testable solution) throws Exception {
		ExerciseBase.rides = rides;
		ExerciseBase.fares = fares;

		return execute(sink, exercise, solution);
	}

	private List<OUT> execute(TestSink<OUT> sink, Testable exercise, Testable solution) throws Exception {
		sink.values.clear();

		ExerciseBase.out = sink;
		ExerciseBase.parallelism = 1;

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
