package com.dataartisans.flinktraining.exercises.datastream_java.broadcast;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.testing.TaxiRideTestBase;
import com.dataartisans.flinktraining.solutions.datastream_java.broadcast.TaxiQuerySolution;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TaxiQueryExerciseTest extends TaxiRideTestBase<Tuple2<String, String>> {

	static Testable javaExercise = () -> TaxiQueryExercise.main(new String[]{});


	@Test
	public void ongoingRides() throws Exception {
		TaxiRide rideStarted = startRide(1, minutes(0), 0, 0, 1);

		TestRideSource rides = new TestRideSource(rideStarted);
		TestStringSource queries = new TestStringSource("ride.isStart");

		List<String> expected = Lists.newArrayList(rideStarted.toString());
		assertEquals(expected, javaResults(rides, queries));
	}

	@Test
	public void nearbyAvailableTaxis() throws Exception {
		final float pennStationLon = -73.9947F;
		final float pennStationLat = 40.750626F;
		final float momaLon = -73.9776F;
		final float momaLat = 40.7614F;

		TaxiRide rideStarted = startRide(1, minutes(0), pennStationLon, pennStationLat, 1);
		TaxiRide rideEnded = endRide(rideStarted, minutes(5), momaLon, momaLat);

		TestRideSource rides = new TestRideSource(rideStarted, rideEnded);
		TestStringSource queries = new TestStringSource(String.format("ride.getEuclideanDistance(%f, %f) < 1.0", momaLon, momaLat));

		List<String> expected = Lists.newArrayList(rideEnded.toString());
		assertEquals(expected, javaResults(rides, queries));
	}

	private DateTime minutes(int n) {
		return new DateTime(2000, 1, 1, 0, 0).plusMinutes(n);
	}

	private TaxiRide testRide(long rideId, Boolean isStart, DateTime startTime, DateTime endTime, float startLon, float startLat, float endLon, float endLat, long taxiId) {
		return new TaxiRide(rideId, isStart, startTime, endTime, startLon, startLat, endLon, endLat, (short) 1, taxiId, 0);
	}

	private TaxiRide startRide(long rideId, DateTime startTime, float startLon, float startLat, long taxiId) {
		return testRide(rideId, true, startTime, new DateTime(0), startLon, startLat, 0, 0, taxiId);
	}

	private TaxiRide endRide(TaxiRide started, DateTime endTime, float endLon, float endLat) {
		return testRide(started.rideId, false, started.startTime, endTime, started.startLon, started.startLat, endLon, endLat, started.taxiId);
	}

	private List<String> javaResults(TestRideSource rides, TestStringSource queries) throws Exception {
		Testable javaSolution = () -> TaxiQuerySolution.main(new String[]{});
		List<Tuple2<String, String>>results = runApp(rides, queries, new TestSink<>(), javaExercise, javaSolution);

		ArrayList<String> rideStrings = new ArrayList<>(results.size());
		results.iterator().forEachRemaining((Tuple2<String, String> t) -> {
				if (t.f0 != "QUERY") {
					rideStrings.add(t.f1);
				}
		});
		return rideStrings;
	}

}