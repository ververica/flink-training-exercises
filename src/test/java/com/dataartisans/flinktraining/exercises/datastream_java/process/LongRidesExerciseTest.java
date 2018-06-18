package com.dataartisans.flinktraining.exercises.datastream_java.process;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.testing.MissingExercise;
import com.dataartisans.flinktraining.exercises.datastream_java.testing.TaxiRideTestBase;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.MissingSolutionException;
import com.dataartisans.flinktraining.solutions.datastream_java.process.LongRidesSolution;
import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class LongRidesExerciseTest extends TaxiRideTestBase<TaxiRide> {
	static Testable javaExercise = () -> LongRidesExercise.main(new String[]{});
	static Testable javaSolution = () -> LongRidesSolution.main(new String[]{});

	static Testable scalaExercise = () -> com.dataartisans.flinktraining.exercises.datastream_scala.process.LongRidesExercise.main(new String[]{});
	static Testable scalaSolution = () -> com.dataartisans.flinktraining.solutions.datastream_scala.process.LongRidesSolution.main(new String[]{});

	static Testable javaCEPSolution = () -> com.dataartisans.flinktraining.solutions.datastream_java.cep.LongRidesSolution.main(new String[]{});

	static Testable scalaCEPSolution = () -> com.dataartisans.flinktraining.solutions.datastream_scala.cep.LongRidesSolution.main(new String[]{});

	public List<TaxiRide> javaResults(TestSource source) throws Exception {
		return runRidesTest(source, new TestSink<TaxiRide>(), javaExercise, javaSolution);
	}

	public List<TaxiRide> scalaResults(TestSource source) throws Exception {
		return runRidesTest(source, new TestSink<TaxiRide>(), scalaExercise, scalaSolution);
	}

	public List<TaxiRide> javaCEPResults(TestSource source) throws Exception {
		return runRidesTest(source, new TestSink<TaxiRide>(), missingExercise, javaCEPSolution);
	}

	public List<TaxiRide> scalaCEPResults(TestSource source) throws Exception {
		return runRidesTest(source, new TestSink<TaxiRide>(), missingExercise, scalaCEPSolution);
	}

	private DateTime beginning = new DateTime(2000, 1, 1, 0, 0);
	TestSink<TaxiRide> sink = new TestSink<TaxiRide>();

	@Test
	public void shortRide() throws Exception {
		DateTime oneMinLater = beginning.plusMinutes(1);
		TaxiRide rideStarted = startRide(1, beginning);
		TaxiRide endedOneMinLater = endRide(rideStarted, oneMinLater);
		Long markOneMinLater = oneMinLater.getMillis();

		TestSource source = new TestSource(rideStarted, endedOneMinLater, markOneMinLater);
		assert(javaResults(source).isEmpty());
		assert(scalaResults(source).isEmpty());
		assert(javaCEPResults(source).isEmpty());
		assert(scalaCEPResults(source).isEmpty());
	}

	@Test
	public void outOfOrder() throws Exception {
		DateTime oneMinLater = beginning.plusMinutes(1);
		TaxiRide rideStarted = startRide(1, beginning);
		TaxiRide endedOneMinLater = endRide(rideStarted, oneMinLater);
		Long markOneMinLater = oneMinLater.getMillis();

		TestSource source = new TestSource(endedOneMinLater, rideStarted, markOneMinLater);
		assert(javaResults(source).isEmpty());
		assert(scalaResults(source).isEmpty());
		assert(javaCEPResults(source).isEmpty());
		assert(scalaCEPResults(source).isEmpty());
	}

	@Test
	public void noStartShort() throws Exception {
		DateTime oneMinLater = beginning.plusMinutes(1);
		TaxiRide rideStarted = startRide(1, beginning);
		TaxiRide endedOneMinLater = endRide(rideStarted, oneMinLater);
		Long markOneMinLater = oneMinLater.getMillis();

		TestSource source = new TestSource(endedOneMinLater, markOneMinLater);
		assert(javaResults(source).isEmpty());
		assert(scalaResults(source).isEmpty());
		assert(javaCEPResults(source).isEmpty());
		assert(scalaCEPResults(source).isEmpty());
	}

	@Test
	public void noEnd() throws Exception {
		TaxiRide rideStarted = startRide(1, beginning);
		Long markThreeHoursLater = beginning.plusHours(3).getMillis();

		TestSource source = new TestSource(rideStarted, markThreeHoursLater);
		assertEquals(Lists.newArrayList(rideStarted), javaResults(source));
		assertEquals(Lists.newArrayList(rideStarted), scalaResults(source));
		assertEquals(Lists.newArrayList(rideStarted), javaCEPResults(source));
		assertEquals(Lists.newArrayList(rideStarted), scalaCEPResults(source));
	}

	@Test
	public void longRide() throws Exception {
		TaxiRide rideStarted = startRide(1, beginning);
		Long mark2HoursLater = beginning.plusMinutes(120).getMillis();
		TaxiRide rideEnded3HoursLater = endRide(rideStarted, beginning.plusHours(3));

		TestSource source = new TestSource(rideStarted, mark2HoursLater, rideEnded3HoursLater);
		assertEquals(Lists.newArrayList(rideStarted), javaResults(source));
		assertEquals(Lists.newArrayList(rideStarted), scalaResults(source));
		assertEquals(Lists.newArrayList(rideStarted), javaCEPResults(source));
		assertEquals(Lists.newArrayList(rideStarted), scalaCEPResults(source));
	}

	private TaxiRide testRide(long rideId, Boolean isStart, DateTime startTime, DateTime endTime) {
		return new TaxiRide(rideId, isStart, startTime, endTime, -73.9947F, 40.750626F, -73.9947F, 40.750626F, (short)1, 0, 0);
	}

	private TaxiRide startRide(long rideId, DateTime startTime) {
		return testRide(rideId, true, startTime, new DateTime(0));
	}

	private TaxiRide endRide(TaxiRide started, DateTime endTime) {
		return testRide(started.rideId, false, started.startTime, endTime);
	}

}