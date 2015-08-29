/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.	See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.	The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.	You may obtain a copy of the License at
 *
 *		 http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.joda.time.DateTime;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Calendar;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Random;

/**
 *
 */
public class AccidentGenerator implements SourceFunction<Accident> {

	private static int MAX_MINS_TO_CLEARANCE = 120;
	private static double ACCIDENT_PER_MIN_PROB = 0.4;

	private float servingSpeedFactor;

	public AccidentGenerator() {
		this(1.0f);
	}

	public AccidentGenerator(float servingSpeedFactor) {
		this.servingSpeedFactor = servingSpeedFactor;
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

			if(rand.nextDouble() < ACCIDENT_PER_MIN_PROB) {
				// new accident

				float lon = GeoUtils.getRandomNYCLon(rand);
				float lat = GeoUtils.getRandomNYCLat(rand);
				int minutesToClearance = rand.nextInt(MAX_MINS_TO_CLEARANCE);

				Accident accident = new Accident(accidentCnt, lon, lat, false);
				accidentCnt++;
				// emit accident start record
				sourceContext.collect(accident);
				// store accident end record
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

