package com.dataartisans.flinktraining.exercises.datastream_java.basics;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.MissingSolutionException;
import com.dataartisans.flinktraining.exercises.datastream_java.testing.TaxiRideTestBase;
import com.google.common.collect.Lists;
import org.apache.flink.runtime.client.JobExecutionException;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RideCleansingTest extends TaxiRideTestBase<TaxiRide> {
	TestSink<TaxiRide> sink = new TestSink<TaxiRide>();

	@Test
	public void testCleansingFilter() throws Exception {

		TaxiRide atPennStation = testRide(-73.9947F, 40.750626F, -73.9947F, 40.750626F);
		TaxiRide toThePole = testRide(-73.9947F, 40.750626F, 0, 90);
		TaxiRide fromThePole = testRide(0, 90, -73.9947F, 40.750626F);
		TaxiRide atNorthPole = testRide(0, 90, 0, 90);

		TestSource source = new TestSource(atPennStation, toThePole, fromThePole, atNorthPole);
		runTest(source, sink);
		assertEquals(Lists.newArrayList(atPennStation), sink.values);
	}

	private TaxiRide testRide(float startLon, float startLat, float endLon, float endLat) {
		return new TaxiRide(1L, true, new DateTime(0), new DateTime(0),
				startLon, startLat, endLon, endLat, (short)1, 0, 0);
	}

	private void runTest(TestSource source, TestSink<TaxiRide> sink) throws Exception {
		sink.values.clear();

		try {
			RideCleansingExercise.in = source;
			RideCleansingExercise.out = sink;
			RideCleansingExercise.parallelism = 1;
			RideCleansingExercise.main(new String[]{});
		} catch (JobExecutionException | MissingSolutionException e) {
			if (e instanceof MissingSolutionException ||
					(e.getCause() != null && e.getCause() instanceof MissingSolutionException)) {
				RideCleansingSolution.in = source;
				RideCleansingSolution.out = sink;
				RideCleansingSolution.parallelism = 1;
				RideCleansingSolution.main(new String[]{});
			} else {
				throw e;
			}
		}
	}
}