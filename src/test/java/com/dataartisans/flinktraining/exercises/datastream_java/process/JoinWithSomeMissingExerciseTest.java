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

package com.dataartisans.flinktraining.exercises.datastream_java.process;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.testing.TaxiRideTestBase;
import com.dataartisans.flinktraining.solutions.datastream_java.process.JoinWithSomeMissingSolution;
import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class JoinWithSomeMissingExerciseTest extends TaxiRideTestBase<TaxiFare> {

	static Testable javaExercise = () -> JoinWithSomeMissingExercise.main(new String[]{});
	static Testable scalaExercise = () -> com.dataartisans.flinktraining.exercises.datastream_scala.process.JoinWithSomeMissingExercise.main(new String[]{});


	final TaxiRide ride1 = testRide(1);
	final TaxiFare fare1 = testFare(1);
	final TaxiFare fare2 = testFare(2);

	@Test
	public void testFareIsUnmatched() throws Exception {
		TestRideSource rides = new TestRideSource(ride1);
		TestFareSource fares = new TestFareSource(fare1, fare2);

		ArrayList<TaxiFare> unmatched = Lists.newArrayList(
					fare2
				);

		assertEquals(unmatched, javaResults(rides, fares));
		assertEquals(unmatched, scalaResults(rides, fares));
	}

	@Test
	public void testOrderDoesNotMatter() throws Exception {
		TestRideSource rides = new TestRideSource(ride1);
		TestFareSource fares = new TestFareSource(fare2, fare1);

		ArrayList<TaxiFare> unmatched = Lists.newArrayList(
				fare2
		);

		assertEquals(unmatched, javaResults(rides, fares));
		assertEquals(unmatched, scalaResults(rides, fares));
	}

	private TaxiRide testRide(long rideId) {
		return new TaxiRide(rideId, true, new DateTime(0), new DateTime(0),
				0F, 0F, 0F, 0F, (short)1, 0, rideId);
	}

	private TaxiFare testFare(long rideId) {
		return new TaxiFare(rideId, 0, rideId, new DateTime(0), "", 0F, 0F, 0F);
	}

	private List<?> javaResults(TestRideSource rides, TestFareSource fares) throws Exception {
		Testable javaSolution = () -> JoinWithSomeMissingSolution.main(new String[]{});
		return runApp(rides, fares, new TestSink<TaxiFare>(), javaExercise, javaSolution);
	}

	private List<?> scalaResults(TestRideSource rides, TestFareSource fares) throws Exception {
		Testable scalaSolution = () -> com.dataartisans.flinktraining.solutions.datastream_scala.process.JoinWithSomeMissingSolution.main(new String[]{});
		return runApp(rides, fares, new TestSink<TaxiFare>(), scalaExercise, scalaSolution);
	}

}