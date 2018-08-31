/*
 * Copyright 2018 data Artisans GmbH
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
import com.dataartisans.flinktraining.exercises.datastream_java.sources.FinSources;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/*
 * This is a processing-time-based enrichment join implemented with a CoProcessFunction,
 * used to enrich a stream of financial Trades with Customer data.
 *
 * When we receive a Trade we immediately join it with whatever Customer data is
 * available. If nothing is known about this Customer, we wait until the current
 * watermark reaches the timestamp of the Trade and try again.
 *
 * The implementation assumes only one Trade per Customer per timestamp (typically, per millisecond).
 *
 * Exercise goals:
 *
 * Extend the implementation to handle multiple Trades with the same timestamp by changing
 *
 *
 *		MapState<Long, Trade> tradeMap
 *
 * to
 *
 * 		MapState<Long, List<Trade>> tradeMap
 */

public class ProcessingTimeJoinExercise {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// Simulated trade stream
		DataStream<Trade> tradeStream = FinSources.tradeSource(env);

		// Simulated customer stream
		DataStream<Customer> customerStream = FinSources.customerSource(env);

		// Stream of enriched trades
		DataStream<EnrichedTrade> joinedStream = tradeStream
				.keyBy("customerId")
				.connect(customerStream.keyBy("customerId"))
				.process(new ProcessingTimeJoinFunction());

		joinedStream.print();

		env.execute("processing-time join");
	}

	public static class ProcessingTimeJoinFunction extends CoProcessFunction<Trade, Customer, EnrichedTrade> {
		// Store pending Trades for a customerId, keyed by timestamp
		private MapState<Long, Trade> tradeMap = null;

		// Store latest Customer update
		private ValueState<Customer> customerState = null;

		@Override
		public void open(Configuration config) {
			MapStateDescriptor<Long, Trade> tDescriptor = new MapStateDescriptor<>(
					"tradeBuffer",
					TypeInformation.of(Long.class),
					TypeInformation.of(Trade.class)
			);
			tradeMap = getRuntimeContext().getMapState(tDescriptor);

			ValueStateDescriptor<Customer> cDescriptor = new ValueStateDescriptor<>(
					"customer",
					TypeInformation.of(Customer.class)
			);
			customerState = getRuntimeContext().getState(cDescriptor);
		}

		@Override
		public void processElement1(Trade trade,
									Context context,
									Collector<EnrichedTrade> out)
				throws Exception {

			System.out.println("Received " + trade.toString());

			if (customerState.value() != null) {
				out.collect(new EnrichedTrade(trade, customerState.value()));
			} else {
				TimerService timerService = context.timerService();

				if (context.timestamp() > timerService.currentWatermark()) {
					// Do the join later, by which time a relevant Customer records will hopefully have arrived.
					tradeMap.put(trade.timestamp, trade);
					timerService.registerEventTimeTimer(trade.timestamp);
				} else {
					// Late Trades land here.
				}
			}
		}

		@Override
		public void processElement2(Customer customer,
									Context context,
									Collector<EnrichedTrade> collector)
				throws Exception {

			System.out.println("Received " + customer.toString());
			customerState.update(customer);
		}

		@Override
		public void onTimer(long t,
							OnTimerContext context,
							Collector<EnrichedTrade> out)
				throws Exception {

			Trade trade = tradeMap.get(t);
			if (trade != null) {
				tradeMap.remove(t);
				out.collect(new EnrichedTrade(trade, customerState.value()));
			}
		}
	}
}
