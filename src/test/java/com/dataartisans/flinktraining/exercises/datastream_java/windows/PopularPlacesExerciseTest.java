package com.dataartisans.flinktraining.exercises.datastream_java.windows;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.testing.TaxiRideTestBase;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple5;
import org.joda.time.DateTime;
import org.junit.Test;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class PopularPlacesExerciseTest extends TaxiRideTestBase<Tuple5<Float, Float, Long, Boolean, Integer>> {
	static Testable javaExercise = () -> PopularPlacesExercise.main(new String[]{"-threshold", "2"});
	static Testable javaSolution = () -> com.dataartisans.flinktraining.solutions.datastream_java.PopularPlacesSolution.main(new String[]{"-threshold", "2"});

	static Testable scalaExercise = () -> com.dataartisans.flinktraining.exercises.datastream_scala.windows.PopularPlacesExercise.main(new String[]{"-threshold", "2"});
	static Testable scalaSolution = () -> com.dataartisans.flinktraining.solutions.datastream_scala.PopularPlacesSolution.main(new String[]{"-threshold", "2"});

	public TestSink<Tuple5<Float, Float, Long, Boolean, Integer>> javaResults(TestSource source) throws Exception {
		TestSink<Tuple5<Float, Float, Long, Boolean, Integer>> sink = new TestSink<>();
		runTest(source, sink, javaExercise, javaSolution);
		return sink;
	}

	public TestSink<Tuple5<Float, Float, Long, Boolean, Integer>> scalaResults(TestSource source) throws Exception {
		TestSink<Tuple5<Float, Float, Long, Boolean, Integer>> sink = new TestSink<>();
		runTest(source, sink, scalaExercise, scalaSolution);
		return sink;
	}

	float pennStationLon = -73.9947F;
	float pennStationLat = 40.750626F;
	float momaLon = -73.9776F;
	float momaLat = 40.7614F;

	DateTime zero = new DateTime(2000, 1, 1, 0, 0);
	DateTime six = zero.plusMinutes(6);
	DateTime fourteen = zero.plusMinutes(14);
	DateTime fifteen = zero.plusMinutes(15);

	@Test
	public void testPopularPlaces() throws Exception {
		TaxiRide penn0 = startRide(1, zero, pennStationLon, pennStationLat);
		TaxiRide penn6 = startRide(2, six, pennStationLon, pennStationLat);
		TaxiRide penn14 = startRide(3, fourteen, pennStationLon, pennStationLat);
		TaxiRide moma15a = endRide(penn0, fifteen, momaLon, momaLat);
		TaxiRide moma15b = endRide(penn6, fifteen, momaLon, momaLat);
		TaxiRide moma15c = endRide(penn14, fifteen, momaLon, momaLat);

		TestSource source = new TestSource(
				penn0, t(0), t(5),
				penn6, t(10),
				penn14,
				moma15a, moma15b, moma15c, t(15), t(20), t(25), t(30), t(35));

		int momaGridId = GeoUtils.mapToGridCell(momaLon, momaLat);
		float momaGridLon = GeoUtils.getGridCellCenterLon(momaGridId);
		float momaGridLat = GeoUtils.getGridCellCenterLat(momaGridId);

		Tuple5<Float, Float, Long, Boolean, Integer> penn10 = new Tuple5<>(pennStationLon, pennStationLat, t(10), true, 2);
		Tuple5<Float, Float, Long, Boolean, Integer> penn15 = new Tuple5<>(pennStationLon, pennStationLat, t(15), true, 3);
		Tuple5<Float, Float, Long, Boolean, Integer> penn20 = new Tuple5<>(pennStationLon, pennStationLat, t(20), true, 2);
		Tuple5<Float, Float, Long, Boolean, Integer> moma20 = new Tuple5<>(momaGridLon, momaGridLat, t(20), false, 3);
		Tuple5<Float, Float, Long, Boolean, Integer> moma25 = new Tuple5<>(momaGridLon, momaGridLat, t(25), false, 3);
		Tuple5<Float, Float, Long, Boolean, Integer> moma30 = new Tuple5<>(momaGridLon, momaGridLat, t(30), false, 3);

		ArrayList<Tuple5<Float, Float, Long, Boolean, Integer>> expected = Lists.newArrayList(penn10, penn15, penn20, moma20, moma25, moma30);

		assertEquals(expected, javaResults(source).values);
		assertEquals(scalaTuples(expected), scalaResults(source).values);
	}

	private long t(int n) {
		return zero.plusMinutes(n).getMillis();
	}

	// setting the endLon and endLat to the same as the starting position; shouldn't matter
	private TaxiRide startRide(long rideId, DateTime startTime, float startLon, float startLat) {
		return new TaxiRide(rideId, true, startTime, new DateTime(0), startLon, startLat, startLon, startLat, (short) 1, 0, 0);
	}

	private TaxiRide endRide(TaxiRide started, DateTime endTime, float endLon, float endLat) {
		return new TaxiRide(started.rideId, false, started.startTime, endTime,
				started.startLon, started.startLat, endLon, endLat, (short) 1, 0, 0);
	}

	private ArrayList<scala.Tuple5<Float, Float, Long, Boolean, Integer>> scalaTuples(ArrayList<Tuple5<Float, Float, Long, Boolean, Integer>> a) {
		ArrayList<scala.Tuple5<Float, Float, Long, Boolean, Integer>> scalaCopy = new ArrayList<>(a.size());
		a.iterator().forEachRemaining(t -> scalaCopy.add(new scala.Tuple5(t.f0, t.f1, t.f2, t.f3, t.f4)));
		return scalaCopy;
	}

}