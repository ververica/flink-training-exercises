package com.dataartisans.flinktraining.exercises.datastream_java.state;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.testing.TaxiRideTestBase;
import com.dataartisans.flinktraining.solutions.datastream_java.state.RidesAndFaresSolution;
import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class RidesAndFaresExerciseTest extends TaxiRideTestBase<TaxiRide> {
	static Testable javaExercise = () -> RidesAndFaresExercise.main(new String[]{});
	static Testable javaSolution = () -> RidesAndFaresSolution.main(new String[]{});

//	static Testable scalaExercise = () -> com.dataartisans.flinktraining.exercises.datastream_scala.basics.RideCleansingExercise.main(new String[]{});
//	static Testable scalaSolution = () -> com.dataartisans.flinktraining.solutions.datastream_scala.basics.RideCleansingSolution.main(new String[]{});
//
	public List<?> javaResults(TestRideSource rides, TestFareSource fares) throws Exception {
		return runTest(rides, fares, new TestSink<TaxiRide>(), javaExercise, javaSolution);
	}

//	public List<?> scalaResults(TestRideSource rides, TestFareSource fares) throws Exception {
//		return runTwoInputTest(rides, fares, new TestSink<TaxiRide>(), scalaExercise, scalaSolution);
//	}

	@Test
	public void testEnrichment() throws Exception {

//		TestRideSource rides = new TestRideSource();
//		TestFareSource fares = new TestFareSource();
//
//		assertEquals(Lists.newArrayList(atPennStation), javaResults(source));
	}

	private TaxiRide testRide(float startLon, float startLat, float endLon, float endLat) {
		return new TaxiRide(1L, true, new DateTime(0), new DateTime(0),
				startLon, startLat, endLon, endLat, (short)1, 0, 0);
	}

}