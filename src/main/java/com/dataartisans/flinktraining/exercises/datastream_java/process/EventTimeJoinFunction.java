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

package com.dataartisans.flinktraining.exercises.datastream_java.process;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.Customer;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.EnrichedTrade;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.Trade;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * This is a homegrown join function using the new Flink 1.2 ProcessFunction.
 * Basically, what we do is the following:
 *
 * 1) When we receive a trade we join it against the customer data right away, however
 * to keep this 100% deterministic we join against the latest customer data that
 * has a timestamp LESS THAN the trade timestamp -- not simply the latest available data.
 * In other words we are joining against the customer data that we knew at the time of the trade.
 *
 * 2) We also set a trigger to evaluate the trade again once the watermark has passed the trade
 * time.  Basically, what we're doing here is using event time to ensure that we have
 * "complete" data and then joining again at that time. This will give us a deterministic
 * result even in the face of undordered data, etc
 *
 * This approach has the benefit that you don't introduce any latency into the trade stream
 * because you always join right away.  You then emit a BETTER result if you receive better
 * information.  We use event time in order to know how long we must wait for this potential
 * better information.
 *
 * We also use event time to know when it's safe to expire state.
 */

public class EventTimeJoinFunction extends EventTimeJoinHelper {
	@Override
	public void processElement1(Trade trade, Context context, Collector<EnrichedTrade> collector) throws Exception {
		System.out.println("Java Received " + trade.toString());
		TimerService timerService = context.timerService();
		EnrichedTrade joinedData = join(trade);
		collector.collect(joinedData);

		if (context.timestamp() > timerService.currentWatermark()) {
			enqueueEnrichedTrade(joinedData);
			timerService.registerEventTimeTimer(trade.timestamp);
		} else {
			// Handle late data -- detect and join against what, latest?  Drop it?
		}
	}

	@Override
	public void processElement2(Customer customer, Context context, Collector<EnrichedTrade> collector) throws Exception {
		System.out.println("Java Received " + customer.toString());
		enqueueCustomer(customer);
	}

	@Override
	public void onTimer(long l, OnTimerContext context, Collector<EnrichedTrade> collector) throws Exception {
		// look for trades that can now be completed, do the join, and remove from the tradebuffer
		Long watermark = context.timerService().currentWatermark();
		while (timestampOfFirstTrade() <= watermark) {
			dequeueAndPerhapsEmit(collector);
		}

		// Cleanup all the customer data that is eligible
		cleanupEligibleCustomerData(watermark);
	}

	private EnrichedTrade join(Trade trade) throws Exception {
		return new EnrichedTrade(trade, getCustomerInfo(trade));
	}

	private void dequeueAndPerhapsEmit(Collector<EnrichedTrade> collector) throws Exception {
		EnrichedTrade enrichedTrade = dequeueEnrichedTrade();

		EnrichedTrade joinedData = join(enrichedTrade.trade);
		// Only emit again if we have better data
		if (!joinedData.equals(enrichedTrade)) {
			collector.collect(joinedData);
		}
	}
}
