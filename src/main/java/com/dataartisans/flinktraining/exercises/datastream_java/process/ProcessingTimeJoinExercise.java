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
 * This is the simplest possible enrichment join, used to enrich a stream of financial Trades
 * with Customer data. When we receive a Trade we immediately join it with whatever Customer
 * data is available.
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
		// Store latest Customer update
		private ValueState<Customer> customerState = null;

		@Override
		public void open(Configuration config) {
			ValueStateDescriptor<Customer> cDescriptor = new ValueStateDescriptor<>(
					"customer",
					TypeInformation.of(Customer.class)
			);
			customerState = getRuntimeContext().getState(cDescriptor);
		}

		@Override
		public void processElement1(Trade trade,
									Context context,
									Collector<EnrichedTrade> out) throws Exception {
			out.collect(new EnrichedTrade(trade, customerState.value()));
		}

		@Override
		public void processElement2(Customer customer,
									Context context,
									Collector<EnrichedTrade> collector) throws Exception {
			customerState.update(customer);
		}
	}
}
