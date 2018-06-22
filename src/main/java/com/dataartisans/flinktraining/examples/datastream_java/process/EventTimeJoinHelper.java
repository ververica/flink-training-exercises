/*
 * Copyright 2017 data Artisans GmbH
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

package com.dataartisans.flinktraining.examples.datastream_java.process;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.Customer;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.EnrichedTrade;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.Trade;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;

import java.util.Collections;
import java.util.PriorityQueue;

abstract class EventTimeJoinHelper extends CoProcessFunction<Trade, Customer, EnrichedTrade> {

	private ValueState<PriorityQueue<EnrichedTrade>> tradeBufferState = null;
	private ValueState<PriorityQueue<Customer>> customerBufferState = null;

	@Override
	public void open(Configuration config) {
		ValueStateDescriptor<PriorityQueue<EnrichedTrade>> tDescriptor = new ValueStateDescriptor<>(
				"tradeBuffer",
				TypeInformation.of(new TypeHint<PriorityQueue<EnrichedTrade>>() {
				}));
		tradeBufferState = getRuntimeContext().getState(tDescriptor);

		ValueStateDescriptor<PriorityQueue<Customer>> cDescriptor = new ValueStateDescriptor<>(
				"customerBuffer",
				TypeInformation.of(new TypeHint<PriorityQueue<Customer>>() {
				}));
		customerBufferState = getRuntimeContext().getState(cDescriptor);
	}

	protected Long timestampOfFirstTrade() throws Exception {
		PriorityQueue<EnrichedTrade> tradeBuffer = tradeBufferState.value();
		EnrichedTrade first = tradeBuffer.peek();
		if (first == null) {
			return Long.MAX_VALUE;
		} else {
			return first.trade.timestamp;
		}
	}

	protected String getCustomerInfo(Trade trade) throws Exception {
		PriorityQueue<Customer> copy = new PriorityQueue<>(customerBufferState.value());

		while(!copy.isEmpty()) {
			Customer c = copy.poll();
			if (c.timestamp <= trade.timestamp) {
				return c.customerInfo;
			}
		}

		return "No customer info available";
	}

	protected void cleanupEligibleCustomerData(Long watermark) throws Exception {
		// Keep all the customer data that is newer than the watermark PLUS
		// the most recent element that is older than the watermark.

		PriorityQueue<Customer> customerBuffer = customerBufferState.value();
		PriorityQueue<Customer> newEnough = new PriorityQueue<Customer>(10, Collections.reverseOrder());

		while(!customerBuffer.isEmpty()) {
			Customer customer = customerBuffer.poll();
			newEnough.add(customer);
			if (customer.timestamp < watermark) {
				break;
			}
		}
		customerBufferState.update(newEnough);
	}

	protected EnrichedTrade dequeueEnrichedTrade() throws Exception {
		PriorityQueue<EnrichedTrade> tradeBuffer = tradeBufferState.value();
		EnrichedTrade enrichedTrade = tradeBuffer.poll();
		tradeBufferState.update(tradeBuffer);
		return enrichedTrade;
	}

	protected void enqueueEnrichedTrade(EnrichedTrade joinedData) throws Exception {
		PriorityQueue<EnrichedTrade> tradeBuffer = tradeBufferState.value();
		// order trades from earliest to latest
		if (tradeBuffer == null) {
			tradeBuffer = new PriorityQueue<EnrichedTrade>();
		}
		tradeBuffer.add(joinedData);
		tradeBufferState.update(tradeBuffer);
	}

	protected void enqueueCustomer(Customer customer) throws Exception {
		PriorityQueue<Customer> customerBuffer = customerBufferState.value();
		// order customers from latest to earliest
		if (customerBuffer == null) {
			customerBuffer = new PriorityQueue<Customer>(10, Collections.reverseOrder());
		}
		customerBuffer.add(customer);
		customerBufferState.update(customerBuffer);
	}
}
