/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Calendar;
import java.util.Locale;

// tripId, time, event[START|END], startLon, startLat, destLon, destLat, passengerCnt, tripDistance

public class TaxiRideGenerator implements SourceFunction<TaxiRide> {

	private String dataFilePath;
	private float servingSpeedFactor;

	private transient BufferedReader reader;
	private transient DateTimeFormatter timeFormatter;

	private transient long dataStartTime;
	private transient long servingStartTime;

	public TaxiRideGenerator(String dataFilePath) {
		this(dataFilePath, 1.0f);
	}

	public TaxiRideGenerator(String dataFilePath, float servingSpeedFactor) {
		this.dataFilePath = dataFilePath;
		this.servingSpeedFactor = servingSpeedFactor;
	}

	@Override
	public void run(SourceContext<TaxiRide> sourceContext) throws Exception {

		this.servingStartTime = Calendar.getInstance().getTimeInMillis();
		this.timeFormatter = DateTimeFormat.forPattern("yyyy-MM-DD HH:mm:ss").withLocale(Locale.US).withZoneUTC();
		reader = new BufferedReader(new FileReader(new File(dataFilePath)));

		String line;
		if(reader.ready() && (line = reader.readLine()) != null) {
			TaxiRide ride = toRide(line);

			this.dataStartTime = ride.time.getMillis();
			sourceContext.collect(ride);
		}
		else {
			return;
		}

		while(reader.ready() && (line = reader.readLine()) != null) {

			TaxiRide ride = toRide(line);

			long dataDiff = ride.time.getMillis() - this.dataStartTime;
			long servingDiff = Calendar.getInstance().getTimeInMillis() - this.servingStartTime;

			long wait = (long) ((dataDiff / this.servingSpeedFactor) - servingDiff);
			if(wait > 0) {
				Thread.sleep(wait);
			}

			sourceContext.collect(ride);
		}

		this.reader.close();
		this.reader = null;

	}

	@Override
	public void cancel() {

		try {
			if(this.reader != null) {
				this.reader.close();
			}
		}
		catch(IOException ioe) {
			//
		}
		finally {
			this.reader = null;
		}
	}

	private TaxiRide toRide(String line) {

		String[] tokens = line.split(",");
		if(tokens.length != 9) {
			throw new RuntimeException("Invalid record: "+line);
		}

		TaxiRide ride = new TaxiRide();

		try {
			ride.rideId = Long.parseLong(tokens[0]);
			ride.time = DateTime.parse(tokens[1], timeFormatter);
			ride.startLon = tokens[3].length() > 0 ? Float.parseFloat(tokens[3]) : 0.0f;
			ride.startLat = tokens[4].length() > 0 ? Float.parseFloat(tokens[4]) : 0.0f;
			ride.endLon = tokens[5].length() > 0 ? Float.parseFloat(tokens[5]) : 0.0f;
			ride.endLat = tokens[6].length() > 0 ? Float.parseFloat(tokens[6]) : 0.0f;
			ride.passengerCnt = Short.parseShort(tokens[7]);
			ride.travelDistance = tokens[8].length() > 0 ? Float.parseFloat(tokens[8]) : 0.0f;

			if(tokens[2].equals("START")) {
				ride.event = TaxiRide.Event.START;
			}
			else if(tokens[2].equals("END")) {
				ride.event = TaxiRide.Event.END;
			}
			else {
				throw new RuntimeException("Invalid record: "+line);
			}

		}
		catch(NumberFormatException nfe) {
			throw new RuntimeException("Invalid record: "+line, nfe);
		}

		return ride;
	}

}

