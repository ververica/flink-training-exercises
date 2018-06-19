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

package com.dataartisans.flinktraining.exercises.datastream_java.basics;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.testing.TaxiRideTestBase;
import com.dataartisans.flinktraining.exercises.datastream_java.testing.TestSource;
import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class RideCleansingExerciseTest extends TaxiRideTestBase<TaxiRide> {

	static Testable javaExercise = () -> RideCleansingExercise.main(new String[]{});
	static Testable scalaExercise = () -> com.dataartisans.flinktraining.exercises.datastream_scala.basics.RideCleansingExercise.main(new String[]{});


	@Test
	public void testCleansingFilter() throws Exception {

		TaxiRide atPennStation = testRide(-73.9947F, 40.750626F, -73.9947F, 40.750626F);
		TaxiRide toThePole = testRide(-73.9947F, 40.750626F, 0, 90);
		TaxiRide fromThePole = testRide(0, 90, -73.9947F, 40.750626F);
		TaxiRide atNorthPole = testRide(0, 90, 0, 90);

		TestRideSource source = new TestRideSource(atPennStation, toThePole, fromThePole, atNorthPole);

		assertEquals(Lists.newArrayList(atPennStation), javaResults(source));
		assertEquals(Lists.newArrayList(atPennStation), scalaResults(source));
	}

	private TaxiRide testRide(float startLon, float startLat, float endLon, float endLat) {
		return new TaxiRide(1L, true, new DateTime(0), new DateTime(0),
				startLon, startLat, endLon, endLat, (short)1, 0, 0);
	}

	private List<?> javaResults(TestRideSource source) throws Exception {
		Testable javaSolution = () -> com.dataartisans.flinktraining.solutions.datastream_java.basics.RideCleansingSolution.main(new String[]{});
		return runApp(source, new TestSink<TaxiRide>(), javaExercise, javaSolution);
	}

	private List<?> scalaResults(TestRideSource source) throws Exception {
		Testable scalaSolution = () -> com.dataartisans.flinktraining.solutions.datastream_scala.basics.RideCleansingSolution.main(new String[]{});
		return runApp(source, new TestSink<TaxiRide>(), scalaExercise, scalaSolution);
	}

}