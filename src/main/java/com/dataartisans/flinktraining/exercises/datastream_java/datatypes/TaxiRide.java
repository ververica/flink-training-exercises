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

package com.dataartisans.flinktraining.exercises.datastream_java.datatypes;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Locale;

/**
 * A TaxiRide is a taxi ride event. There are two types of events, a taxi ride start event and a
 * taxi ride end event. The isStart flag specifies the type of the event.
 *
 * A TaxiRide consists of
 * - the rideId of the event which is identical for start and end record
 * - the time of the event
 * - the longitude of the start location
 * - the latitude of the start location
 * - the longitude of the end location
 * - the latitude of the end location
 * - the passengerCnt of the ride
 * - the travelDistance which is -1 for start events
 *
 */
public class TaxiRide {

	private static transient DateTimeFormatter timeFormatter =
			DateTimeFormat.forPattern("yyyy-MM-DD HH:mm:ss").withLocale(Locale.US).withZoneUTC();

	public TaxiRide() {}

	public TaxiRide(long rideId, DateTime time, boolean isStart,
					float startLon, float startLat, float endLon, float endLat,
					short passengerCnt, float travelDistance) {

		this.rideId = rideId;
		this.time = time;
		this.isStart = isStart;
		this.startLon = startLon;
		this.startLat = startLat;
		this.endLon = endLon;
		this.endLat = endLat;
		this.passengerCnt = passengerCnt;
		this.travelDistance = travelDistance;
	}

	public long rideId;
	public DateTime time;
	public boolean isStart;
	public float startLon;
	public float startLat;
	public float endLon;
	public float endLat;
	public short passengerCnt;
	public float travelDistance;

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(rideId).append(",");
		sb.append(time.toString(timeFormatter)).append(",");
		sb.append(isStart ? "START" : "END").append(",");
		sb.append(startLon).append(",");
		sb.append(startLat).append(",");
		sb.append(endLon).append(",");
		sb.append(endLat).append(",");
		sb.append(passengerCnt).append(",");
		sb.append(travelDistance);

		return sb.toString();
	}

	public static TaxiRide fromString(String line) {

		String[] tokens = line.split(",");
		if (tokens.length != 9) {
			throw new RuntimeException("Invalid record: " + line);
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
				ride.isStart = true;
			}
			else if(tokens[2].equals("END")) {
				ride.isStart = false;
			}
			else {
				throw new RuntimeException("Invalid record: "+ line);
			}

		} catch (NumberFormatException nfe) {
			throw new RuntimeException("Invalid record: " + line, nfe);
		}

		return ride;
	}

	@Override
	public boolean equals(Object other) {
		if(other instanceof TaxiRide) {
			return this.rideId == ((TaxiRide)other).rideId;
		}
		else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return (int)this.rideId;
	}

}
