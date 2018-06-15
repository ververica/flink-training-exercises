package com.dataartisans.flinktraining.exercises.datastream_java.process;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.testing.TaxiRideTestBase;
import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LongRidesExerciseTest extends TaxiRideTestBase<TaxiRide> {
	static Testable javaExercise = () -> LongRidesExercise.main(new String[]{});
	static Testable javaSolution = () -> com.dataartisans.flinktraining.solutions.datastream_java.LongRidesSolution.main(new String[]{});

	static Testable scalaExercise = () -> com.dataartisans.flinktraining.exercises.datastream_scala.process.LongRidesExercise.main(new String[]{});
	static Testable scalaSolution = () -> com.dataartisans.flinktraining.solutions.datastream_scala.LongRidesSolution.main(new String[]{});

	public TestSink<TaxiRide> javaResults(TestSource source) throws Exception {
		TestSink<TaxiRide> sink = new TestSink<TaxiRide>();
		runTest(source, sink, javaExercise, javaSolution);
		return sink;
	}

	public TestSink<TaxiRide> scalaResults(TestSource source) throws Exception {
		TestSink<TaxiRide> sink = new TestSink<TaxiRide>();
		runTest(source, sink, scalaExercise, scalaSolution);
		return sink;
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
		assert(javaResults(source).values.isEmpty());
		assert(scalaResults(source).values.isEmpty());
	}

	@Test
	public void outOfOrder() throws Exception {
		DateTime oneMinLater = beginning.plusMinutes(1);
		TaxiRide rideStarted = startRide(1, beginning);
		TaxiRide endedOneMinLater = endRide(rideStarted, oneMinLater);
		Long markOneMinLater = oneMinLater.getMillis();

		TestSource source = new TestSource(endedOneMinLater, rideStarted, markOneMinLater);
		assert(javaResults(source).values.isEmpty());
		assert(scalaResults(source).values.isEmpty());
	}

	@Test
	public void noStartShort() throws Exception {
		DateTime oneMinLater = beginning.plusMinutes(1);
		TaxiRide rideStarted = startRide(1, beginning);
		TaxiRide endedOneMinLater = endRide(rideStarted, oneMinLater);
		Long markOneMinLater = oneMinLater.getMillis();

		TestSource source = new TestSource(endedOneMinLater, markOneMinLater);
		assert(javaResults(source).values.isEmpty());
		assert(scalaResults(source).values.isEmpty());
	}

	@Test
	public void noEnd() throws Exception {
		TaxiRide rideStarted = startRide(1, beginning);
		Long markThreeHoursLater = beginning.plusHours(3).getMillis();

		TestSource source = new TestSource(rideStarted, markThreeHoursLater);
		assertEquals(Lists.newArrayList(rideStarted), javaResults(source).values);
		assertEquals(Lists.newArrayList(rideStarted), scalaResults(source).values);
	}

	@Test
	public void longRide() throws Exception {
		TaxiRide rideStarted = startRide(1, beginning);
		Long mark2HoursLater = beginning.plusMinutes(120).getMillis();
		TaxiRide rideEnded3HoursLater = endRide(rideStarted, beginning.plusHours(3));

		TestSource source = new TestSource(rideStarted, mark2HoursLater, rideEnded3HoursLater);
		assertEquals(Lists.newArrayList(rideStarted), javaResults(source).values);
		assertEquals(Lists.newArrayList(rideStarted), scalaResults(source).values);
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