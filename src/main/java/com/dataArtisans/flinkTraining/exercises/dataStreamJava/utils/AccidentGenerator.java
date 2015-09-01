/*
 * Copyright 2015 data Artisans GmbH
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

package com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils;

import com.dataArtisans.flinkTraining.exercises.dataStreamJava.dataTypes.Accident;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Random;

/**
 * Generates a data stream of random Accident event records.
 *
 * An accident event consists of:
 * - an accidentId,
 * - a locations specified by longitude and latitude,
 * - and a flag stating whether the event is an emergence or clearance event.
 *
 * Accidents are randomly placed in the New York City area.
 *
 */
public class AccidentGenerator implements SourceFunction<Accident> {

	private static int DEFAULT_MAX_MINS_TO_CLEARANCE = 120;
	private static double DEFAULT_ACCIDENT_PROB = 0.4;

	private float servingSpeedFactor;
	private int maxMinsToClearance;
	private double accidentsProb;

	public AccidentGenerator() {
		this(1.0f);
	}

	/**
	 * Creates an AccidentGenerator for the given serving speed factor.
	 *
	 * @param servingSpeedFactor Serving speed is adjusted with respect to the serving speed factor.
	 */
	public AccidentGenerator(float servingSpeedFactor) {
		this(servingSpeedFactor, DEFAULT_MAX_MINS_TO_CLEARANCE, DEFAULT_ACCIDENT_PROB);
	}

	/**
	 * Creates an AccidentGenerator. The generator checks each logical minute whether to generate
	 * an accident or not with probability of accidentProb.
	 * Logical time is adjusted to actual time with respect to the serving speed factor.
	 * The time between emergence and clearance of an accident is uniformly distributed between
	 * 0 and maxMinsToClearance minutues.
	 *
	 * @param servingSpeedFactor Serving speed is adjusted with respect to the serving speed factor.
	 * @param maxMinsToClearance Maximum time until an accident is cleared.
	 * @param accidentProb Probability to generate an accident every logical minute.
	 */
	public AccidentGenerator(float servingSpeedFactor, int maxMinsToClearance, double accidentProb) {
		this.servingSpeedFactor = servingSpeedFactor;
		this.maxMinsToClearance = maxMinsToClearance;
		this.accidentsProb = accidentProb;
	}

	@Override
	public void run(SourceContext<Accident> sourceContext) throws Exception {

		long wait = (long)(60000 / servingSpeedFactor) + 1; // one minute scaled

		Random rand = new Random(42l);
		int curMinute = 0;

		long accidentCnt = 0;
		PriorityQueue<Tuple2<Integer, Accident>> accidents =
				new PriorityQueue<Tuple2<Integer, Accident>>(new AccidentTimeComparator());

		while(true) {

			if(rand.nextDouble() < this.accidentsProb) {
				// new accident

				float lon = GeoUtils.getRandomNYCLon(rand);
				float lat = GeoUtils.getRandomNYCLat(rand);
				int minutesToClearance = rand.nextInt(this.maxMinsToClearance);

				Accident accident = new Accident(accidentCnt, lon, lat, false);
				accidentCnt++;
				// emit accident start record
				sourceContext.collect(accident);
				// place accident end record in prio queue ordered by clearance time
				accidents.add(new Tuple2<Integer, Accident>(curMinute+minutesToClearance, accident));
			}

			// emit end records for each cleared accident
			while(accidents.size() > 0 && accidents.peek().f0 <= curMinute) {
				Accident accident = accidents.poll().f1;
				accident.isCleared = true;
				// emit accident end record
				sourceContext.collect(accident);
			}

			// wait for one scaled minute
			Thread.sleep(wait);
			curMinute++;
		}
	}

	@Override
	public void cancel() {
	}

	private static class AccidentTimeComparator implements Comparator<Tuple2<Integer, ?>> {

		@Override
		public int compare(Tuple2<Integer, ?> o1, Tuple2<Integer, ?> o2) {
			return o1.f0 - o2.f0;
		}
	}

}

