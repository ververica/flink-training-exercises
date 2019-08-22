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

package com.ververica.flinktraining.exercises.datastream_java.utils;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.ConnectedCarEvent;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class ConnectedCarAssigner implements AssignerWithPunctuatedWatermarks<ConnectedCarEvent> {
	@Override
	public long extractTimestamp(ConnectedCarEvent event, long previousElementTimestamp) {
		return event.timestamp;
	}

	@Override
	public Watermark checkAndGetNextWatermark(ConnectedCarEvent event, long extractedTimestamp) {
		// simply emit a watermark with every event
		return new Watermark(extractedTimestamp - 30000);
	}
}
