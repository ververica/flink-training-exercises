package com.dataartisans.flinktraining.exercises.datastream_java.utils;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.*;

public abstract class TaxiRideTestBase<OUT> {
	public class TestSource implements SourceFunction<TaxiRide> {
		private volatile boolean running = true;
		private Object[] testStream;

		public TestSource(Object ... ridesOrWatermarks) {
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

	public static class TestSink<OUT> implements SinkFunction<OUT> {

		// must be static
		public static final List values = new ArrayList<>();

		@Override
		public synchronized void invoke(OUT value) throws Exception {
			values.add(value);
		}
	}
}
