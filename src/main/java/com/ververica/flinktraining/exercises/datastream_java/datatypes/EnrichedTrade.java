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

public class EnrichedTrade {

	public EnrichedTrade() {}

	public EnrichedTrade(Trade trade, Customer customer) {
		this.trade = trade;
		this.customer = customer;
	}

	public Trade trade;
	public Customer customer;

	public String toString() {
		String customerInfo;

		if (customer == null) {
			customerInfo = "null";
		} else {
			customerInfo = customer.customerInfo;
		}

		StringBuilder sb = new StringBuilder();
		sb.append("EnrichedTrade(").append(trade.timestamp).append(") ");
		sb.append(customerInfo);
		return sb.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		else if (o != null && getClass() == o.getClass()) {
			EnrichedTrade that = (EnrichedTrade) o;
			return (this.trade.equals(that.trade) &&
					(this.customer == null ? that.customer == null : this.customer.equals(that.customer)));
		}
		return false;
	}
}
