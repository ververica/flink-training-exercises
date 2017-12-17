package com.dataartisans.flinktraining.exercises.datastream_java.basics;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TaxiRideExerciseBase;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collections;

public class RideCleansingExercise extends TaxiRideExerciseBase<TaxiRide> {
	public static void main(String[] args) throws Exception {
		RideCleansingExercise exercise = new RideCleansingExercise();
		exercise.run(exercise.realStream(args));
	}

	@Override
	public DataStream<TaxiRide> perform(DataStream<TaxiRide> input) {
		return input.filter(new RideCleansing.NYCFilter());
	}
}
