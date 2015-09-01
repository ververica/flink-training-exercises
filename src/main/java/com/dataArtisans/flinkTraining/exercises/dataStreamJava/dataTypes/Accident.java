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

package com.dataArtisans.flinkTraining.exercises.dataStreamJava.dataTypes;

/**
 * Accident is an accident report event. There are two types of accident events, an emergence event
 * which reports an accident and a clearance event which notifies that an accident was cleared.
 * The isCleared flag specifies the type of the accident report event.
 *
 * In addition, an accident consists of an accidentId (same for emergence and clearance events) and
 * a location specified by longitude and latitude.
 *
 */
public class Accident {

	public long accidentId;
	public float lon;
	public float lat;
	public boolean isCleared;

	public Accident() {}

	public Accident(long accidentId, float lon, float lat, boolean isCleared) {
		this.accidentId = accidentId;
		this.lon = lon;
		this.lat = lat;
		this.isCleared = isCleared;
	}

}
