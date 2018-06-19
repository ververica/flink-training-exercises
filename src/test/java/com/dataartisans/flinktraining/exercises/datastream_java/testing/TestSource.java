package com.dataartisans.flinktraining.exercises.datastream_java.testing;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

public abstract class TestSource implements SourceFunction {
	private volatile boolean running = true;
	protected Object[] testStream;

	@Override
	public void run(SourceContext ctx) throws Exception {
		for (int i = 0; (i < testStream.length) && running; i++) {
			if (testStream[i] instanceof TaxiRide) {
				TaxiRide ride = (TaxiRide) testStream[i];
				ctx.collectWithTimestamp(ride, ride.getEventTime());
			} else if (testStream[i] instanceof TaxiFare) {
				TaxiFare fare = (TaxiFare) testStream[i];
				ctx.collectWithTimestamp(fare, fare.getEventTime());
			} else if (testStream[i] instanceof String) {
				String s = (String) testStream[i];
				ctx.collectWithTimestamp(s, 0);
			} else if (testStream[i] instanceof Long) {
				Long ts = (Long) testStream[i];
				ctx.emitWatermark(new Watermark(ts));
			} else {
				throw new RuntimeException(testStream[i].toString());
			}
		}
		// test sources are finite, so they have a Long.MAX_VALUE watermark when they finishes
	}

	@Override
	public void cancel() {
		running = false;
	}
}
