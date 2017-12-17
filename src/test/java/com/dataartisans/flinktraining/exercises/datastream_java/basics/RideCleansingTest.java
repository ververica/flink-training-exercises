package com.dataartisans.flinktraining.exercises.datastream_java.basics;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TaxiRideExerciseBase;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collections;

public class RideCleansingTest extends TaxiRideExerciseBase<TaxiRide> {
	TaxiRide pennStation = testRide(-73.9947F, 40.750626F, -73.9947F, 40.750626F);
	TaxiRide northPole = testRide(0, 0, 0, 0);
	TaxiRide toThePole = testRide(-73.9947F, 40.750626F, 0, 0);
	TaxiRide fromThePole = testRide(0, 0, -73.9947F, 40.750626F);

	public void testSolution() throws Exception {
		RideCleansingExercise exercise = new RideCleansingExercise();
		exercise.test(exercise.testStream());
	}

	@Override
	public DataStream<TaxiRide> perform(DataStream<TaxiRide> input) {
		return input.filter(new NYCFilter());
	}

	private TaxiRide testRide(float startLon, float startLat, float endLon, float endLat) {
		return new TaxiRide(1L, true, new DateTime(0), new DateTime(0),
				startLon, startLat, endLon, endLat, (short)1);
	}

	public DataStream<TaxiRide> testStream() {
		return env.fromElements(pennStation, northPole, toThePole, fromThePole);
	}

	@Override
	public ArrayList<TaxiRide> expectedResult() {
		return new ArrayList<TaxiRide>(Collections.singleton(pennStation));
	}

	public static class NYCFilter implements FilterFunction<TaxiRide> {

		@Override
		public boolean filter(TaxiRide taxiRide) throws Exception {

			return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
					GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
		}
	}
}
