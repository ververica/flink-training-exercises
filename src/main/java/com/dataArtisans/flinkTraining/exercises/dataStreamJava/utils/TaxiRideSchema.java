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

import com.dataArtisans.flinkTraining.exercises.dataStreamJava.dataTypes.TaxiRide;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

public class TaxiRideSchema implements DeserializationSchema<TaxiRide>, SerializationSchema<TaxiRide, byte[]> {

	@Override
	public byte[] serialize(TaxiRide element) {
		return element.toString().getBytes();
	}

	@Override
	public TaxiRide deserialize(byte[] message) {
		return TaxiRide.fromString(new String(message));
	}

	@Override
	public boolean isEndOfStream(TaxiRide nextElement) {
		return false;
	}

	@Override
	public TypeInformation<TaxiRide> getProducedType() {
		return TypeExtractor.getForClass(TaxiRide.class);
	}
}