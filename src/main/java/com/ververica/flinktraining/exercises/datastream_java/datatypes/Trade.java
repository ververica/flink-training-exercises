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

public class Trade implements Comparable<Trade> {

	public Trade() {}

	public Trade(Long timestamp, Long customerId, String tradeInfo) {

		this.timestamp = timestamp;
		this.customerId = customerId;
		this.tradeInfo = tradeInfo;
	}

	public Long timestamp;
	public Long customerId;
	public String tradeInfo;

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Trade(").append(timestamp).append(") ");
		sb.append(tradeInfo);
		return sb.toString();
	}

	public int compareTo(Trade other) {
		return Long.compare(this.timestamp, other.timestamp);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		else if (o != null && getClass() == o.getClass()) {
			Trade that = (Trade) o;
			return ((this.customerId.equals(that.customerId)) && (this.timestamp.equals(that.timestamp)));
		}
		return false;
	}

}
