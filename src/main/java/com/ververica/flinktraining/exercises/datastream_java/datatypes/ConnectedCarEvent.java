/*
 * Copyright 2017 data Artisans GmbH, 2019 Ververica GmbH
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

package com.ververica.flinktraining.exercises.datastream_java.datatypes;

import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Iterator;
import java.util.Locale;

/**
 * A ConnectedCarEvent is a car driving event.
 *
 * A ConnectedCarEvent consists of
 * - an id
 * - the time of the event
 * - the longitude
 * - the latitude
 * - the speed
 * - the throttle position (%)
 * - the engine load (%)
 */
public class ConnectedCarEvent implements Comparable<ConnectedCarEvent> {

	public String id;
	public String carId;
	public long timestamp;
	public float longitude;
	public float latitude;
	public float consumption;
	public float speed;
	public float throttle;
	public float engineload;

	private static transient DateTimeFormatter timeFormatter =
			DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ").withLocale(Locale.US).withZoneUTC();

	public ConnectedCarEvent() {}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(id).append(",");
		sb.append(timestamp).append(",");
		sb.append(longitude).append(",");
		sb.append(latitude).append(",");
		sb.append(speed).append(",");
		sb.append(throttle).append(",");
		sb.append(engineload);

		return sb.toString();
	}

	public static ConnectedCarEvent fromString(String line) {

		String[] tokens = line.split("(,|;)\\s*");
		if (tokens.length != 23) {
			throw new RuntimeException("Invalid record: " + line);
		}

		ConnectedCarEvent event = new ConnectedCarEvent();

		try {
			event.id = tokens[1];
			event.carId = tokens[0];
			event.timestamp = DateTime.parse(tokens[22], timeFormatter).getMillis();
			event.longitude = tokens[20].length() > 0 ? Float.parseFloat(tokens[20]) : 0.0f;
			event.latitude = tokens[21].length() > 0 ? Float.parseFloat(tokens[21]) : 0.0f;
			event.consumption = tokens[7].length() > 0 ? Float.parseFloat(tokens[7]) : 0.0f;
			event.speed = tokens[9].length() > 0 ? Float.parseFloat(tokens[9]) : 0.0f;
			event.throttle = tokens[12].length() > 0 ? Float.parseFloat(tokens[12]) : 0.0f;
			event.engineload = tokens[19].length() > 0 ? Float.parseFloat(tokens[19]) : 0.0f;

		} catch (NumberFormatException nfe) {
			throw new RuntimeException("Invalid field: " + line, nfe);
		}

		return event;
	}

	@Override
	public boolean equals(Object other) {
		return other instanceof ConnectedCarEvent &&
				this.id == ((ConnectedCarEvent) other).id;
	}

	@Override
	public int hashCode() {
		return (int)this.id.hashCode();
	}

	public int compareTo(ConnectedCarEvent other) {
		return Long.compare(this.timestamp, other.timestamp);
	}

	public static Long earliestStopElement(Iterable<TimestampedValue<ConnectedCarEvent>> elements) {
		long earliestTime = Long.MAX_VALUE;

		for (Iterator<TimestampedValue<ConnectedCarEvent>> iterator = elements.iterator(); iterator.hasNext(); ) {
			TimestampedValue<ConnectedCarEvent> element = iterator.next();
			if (element.getTimestamp() < earliestTime && element.getValue().speed == 0.0) {
				earliestTime = element.getTimestamp();
			}
		}

		return earliestTime;
	}

}
