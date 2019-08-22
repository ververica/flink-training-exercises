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

import java.util.ArrayList;
import java.util.Iterator;

/**
 * A GapSegment is punctuated by an interval of more than 15 seconds without data.
 */
public class GapSegment extends Segment {
	public GapSegment(Iterable<ConnectedCarEvent> events) {

		ArrayList<ConnectedCarEvent> list = new ArrayList<ConnectedCarEvent>();
		for (Iterator<ConnectedCarEvent> iterator = events.iterator(); iterator.hasNext(); ) {
			ConnectedCarEvent event = iterator.next();
			list.add(event);
		}

		this.length = list.size();

		if (this.length > 0) {
			this.startTime = GapSegment.minTimestamp(list);
			this.maxSpeed = (int) GapSegment.maxSpeed(list);
			this.erraticness = GapSegment.stddevThrottle(list);
		}
	}

}
