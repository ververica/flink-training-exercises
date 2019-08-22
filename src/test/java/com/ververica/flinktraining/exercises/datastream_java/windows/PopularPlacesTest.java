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

package com.ververica.flinktraining.exercises.datastream_java.windows;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.testing.TaxiRideTestBase;
import com.ververica.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.ververica.flinktraining.solutions.datastream_java.windows.PopularPlacesSolution;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple5;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PopularPlacesTest extends TaxiRideTestBase<Tuple5<Float, Float, Long, Boolean, Integer>> {

	static Testable javaExercise = () -> PopularPlacesExercise.main(new String[]{"-threshold", "2"});

	static final float pennStationLon = -73.9947F;
	static final float pennStationLat = 40.750626F;
	static final float momaLon = -73.9776F;
	static final float momaLat = 40.7614F;

	@Test
	public void testPopularPlaces() throws Exception {
		TaxiRide penn0 = startRide(1, t(0), pennStationLon, pennStationLat);
		TaxiRide penn6 = startRide(2, t(6), pennStationLon, pennStationLat);
		TaxiRide penn14 = startRide(3, t(14), pennStationLon, pennStationLat);
		TaxiRide moma15a = endRide(penn0, t(15), momaLon, momaLat);
		TaxiRide moma15b = endRide(penn6, t(15), momaLon, momaLat);
		TaxiRide moma15c = endRide(penn14, t(15), momaLon, momaLat);

		TestRideSource source = new TestRideSource(
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

		assertEquals(expected, results(source));
	}

	protected long t(int n) {
		return new DateTime(2000, 1, 1, 0, 0).plusMinutes(n).getMillis();
	}


	// setting the endLon and endLat to the same as the starting position; shouldn't matter
	protected TaxiRide startRide(long rideId, long startTime, float startLon, float startLat) {
		return new TaxiRide(rideId, true, new DateTime(startTime), new DateTime(0), startLon, startLat, startLon, startLat, (short) 1, 0, 0);
	}

	protected TaxiRide endRide(TaxiRide started, long endTime, float endLon, float endLat) {
		return new TaxiRide(started.rideId, false, started.startTime, new DateTime(endTime),
				started.startLon, started.startLat, endLon, endLat, (short) 1, 0, 0);
	}

	protected List<Tuple5<Float, Float, Long, Boolean, Integer>> results(TestRideSource source) throws Exception {
		Testable javaSolution = () -> PopularPlacesSolution.main(new String[]{"-threshold", "2"});
		return runApp(source, new TestSink<>(), javaExercise, javaSolution);
	}

}