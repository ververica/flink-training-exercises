/*
 * Copyright 2018 data Artisans GmbH, 2019 Ververica GmbH
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

package com.ververica.flinktraining.exercises.datastream_java.process;

import com.ververica.flinktraining.exercises.datastream_java.cep.LongRidesCEPExercise;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.testing.TaxiRideTestBase;
import com.ververica.flinktraining.solutions.datastream_java.cep.LongRidesCEPSolution;
import com.ververica.flinktraining.solutions.datastream_java.process.LongRidesSolution;
import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class LongRidesTest extends TaxiRideTestBase<TaxiRide> {

	static Testable javaExercise = () -> LongRidesExercise.main(new String[]{});
	static Testable javaCEPExercise = () -> LongRidesCEPExercise.main(new String[]{});

	private DateTime beginning = new DateTime(2000, 1, 1, 0, 0);

	@Test
	public void shortRide() throws Exception {
		DateTime oneMinLater = beginning.plusMinutes(1);
		TaxiRide rideStarted = startRide(1, beginning);
		TaxiRide endedOneMinLater = endRide(rideStarted, oneMinLater);
		Long markOneMinLater = oneMinLater.getMillis();

		TestRideSource source = new TestRideSource(rideStarted, endedOneMinLater, markOneMinLater);
		assert(results(source).isEmpty());
		assert(cepResults(source).isEmpty());
	}

	@Test
	public void outOfOrder() throws Exception {
		DateTime oneMinLater = beginning.plusMinutes(1);
		TaxiRide rideStarted = startRide(1, beginning);
		TaxiRide endedOneMinLater = endRide(rideStarted, oneMinLater);
		Long markOneMinLater = oneMinLater.getMillis();

		TestRideSource source = new TestRideSource(endedOneMinLater, rideStarted, markOneMinLater);
		assert(results(source).isEmpty());
		assert(cepResults(source).isEmpty());
	}

	@Test
	public void noStartShort() throws Exception {
		DateTime oneMinLater = beginning.plusMinutes(1);
		TaxiRide rideStarted = startRide(1, beginning);
		TaxiRide endedOneMinLater = endRide(rideStarted, oneMinLater);
		Long markOneMinLater = oneMinLater.getMillis();

		TestRideSource source = new TestRideSource(endedOneMinLater, markOneMinLater);
		assert(results(source).isEmpty());
		assert(cepResults(source).isEmpty());
	}

	@Test
	public void noEnd() throws Exception {
		TaxiRide rideStarted = startRide(1, beginning);
		Long markThreeHoursLater = beginning.plusHours(3).getMillis();

		TestRideSource source = new TestRideSource(rideStarted, markThreeHoursLater);
		assertEquals(Lists.newArrayList(rideStarted), results(source));
		assertEquals(Lists.newArrayList(rideStarted), cepResults(source));
	}

	@Test
	public void longRide() throws Exception {
		TaxiRide rideStarted = startRide(1, beginning);
		Long mark2HoursLater = beginning.plusMinutes(120).getMillis();
		TaxiRide rideEnded3HoursLater = endRide(rideStarted, beginning.plusHours(3));

		TestRideSource source = new TestRideSource(rideStarted, mark2HoursLater, rideEnded3HoursLater);
		assertEquals(Lists.newArrayList(rideStarted), results(source));
		assertEquals(Lists.newArrayList(rideStarted), cepResults(source));
		assertEquals(Lists.newArrayList(rideStarted), checkpointedResults(source));
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

	protected List<TaxiRide> results(TestRideSource source) throws Exception {
		Testable javaSolution = () -> LongRidesSolution.main(new String[]{});
		return runApp(source, new TestSink<>(), javaExercise, javaSolution);
	}

	protected List<TaxiRide> cepResults(TestRideSource source) throws Exception {
		Testable javaCEPSolution = () -> LongRidesCEPSolution.main(new String[]{});
		return runApp(source, new TestSink<>(), javaCEPExercise, javaCEPSolution);
	}

	protected List<TaxiRide> checkpointedResults(TestRideSource source) throws Exception {
		Testable checkpointedSolution = () -> com.ververica.flinktraining.solutions.datastream_java.process.CheckpointedLongRidesSolution.main(new String[]{});
		return runApp(source, new TestSink<>(), checkpointedSolution);
	}

}