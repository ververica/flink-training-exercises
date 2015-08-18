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

import org.joda.time.DateTime;

public class TaxiRide {

  public static enum Event {
		START,
		END
	}

	public TaxiRide() {}

	public TaxiRide(long rideId, DateTime time, Event event,
					float startLon, float startLat, float endLon, float endLat,
					short passengerCnt, float travelDistance) {

		this.rideId = rideId;
		this.time = time;
		this.event = event;
		this.startLon = startLon;
		this.startLat = startLat;
		this.endLon = endLon;
		this.endLat = endLat;
		this.passengerCnt = passengerCnt;
		this.travelDistance = travelDistance;
	}

	public long rideId;
	public DateTime time;
	public Event event;
	public float startLon;
	public float startLat;
	public float endLon;
	public float endLat;
	public short passengerCnt;
	public float travelDistance;

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(rideId).append(",");
		sb.append(time).append(",");
		sb.append(event).append(",");
		sb.append(startLon).append(",");
		sb.append(startLat).append(",");
		sb.append(endLon).append(",");
		sb.append(endLat).append(",");
		sb.append(passengerCnt).append(",");
		sb.append(travelDistance);

		return sb.toString();
	}

  public boolean isStart() {
    return this.event == Event.START;
  }

  public boolean isEnd() {
    return this.event == Event.END;
  }

}
