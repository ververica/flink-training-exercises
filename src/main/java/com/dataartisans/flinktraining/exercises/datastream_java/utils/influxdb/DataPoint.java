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

package com.dataartisans.flinktraining.exercises.datastream_java.utils.influxdb;


public class DataPoint<T> {

	private long timeStampMs;
	private T value;

	public DataPoint() {
		this.timeStampMs = 0;
		this.value = null;
	}

	public DataPoint(long timeStampMs, T value) {
		this.timeStampMs = timeStampMs;
		this.value = value;
	}

	public long getTimeStampMs() {
		return timeStampMs;
	}

	public void setTimeStampMs(long timeStampMs) {
		this.timeStampMs = timeStampMs;
	}

	public T getValue() {
		return value;
	}

	public void setValue(T value) {
		this.value = value;
	}

	public <R> DataPoint<R> withNewValue(R newValue){
		return new DataPoint<>(this.getTimeStampMs(), newValue);
	}

	public <R> KeyedDataPoint<R> withNewKeyAndValue(String key, R newValue){
		return new KeyedDataPoint<>(key, this.getTimeStampMs(), newValue);
	}

	public KeyedDataPoint withKey(String key){
		return new KeyedDataPoint<>(key, this.getTimeStampMs(), this.getValue());
	}

	@Override
	public String toString() {
		return "DataPoint(timestamp=" + timeStampMs + ", value=" + value + ")";
	}
}