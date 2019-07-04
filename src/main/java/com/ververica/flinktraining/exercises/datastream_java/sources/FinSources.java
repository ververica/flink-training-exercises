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

package com.ververica.flinktraining.exercises.datastream_java.sources;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.Customer;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.Trade;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

public class FinSources {
	/**
	 * This source generates the stream of customers
	 *
	 * */
	public static DataStream<Customer> customerSource(StreamExecutionEnvironment env) {
		// This is a bit of a hack to use Thread.sleep() for sequencing but it works for our test purposes
		DataStream<Customer> customers = env.addSource(new SourceFunction<Customer>() {
			private volatile boolean running = true;

			@Override
			public void run(SourceContext<Customer> sc) throws Exception {
				sc.collectWithTimestamp(new Customer(0L, 0L, "customer-0"), 0);
				sc.emitWatermark(new Watermark(0));
				Thread.sleep(2000);
				sc.collectWithTimestamp(new Customer(500L, 0L, "customer-500"), 500);
				sc.emitWatermark(new Watermark(500));
				Thread.sleep(1000);
				sc.collectWithTimestamp(new Customer(1500L, 0L, "customer-1500"), 1500);
				sc.emitWatermark(new Watermark(1500));
				Thread.sleep(6000);
				sc.collectWithTimestamp(new Customer(1600L, 0L, "customer-1600"), 1600);
				sc.emitWatermark(new Watermark(1600));
				Thread.sleep(1000);
				sc.collectWithTimestamp(new Customer(2100L, 0L, "customer-2100"), 2100);
				sc.emitWatermark(new Watermark(2100));

				while (running) {
					Thread.sleep(1000);
				}
			}

			@Override
			public void cancel() {
				running = false;
			}
		});

		return customers;
	}

	/**
	 * This source generates the stream of trades
	 *
	 * */
	public static DataStream<Trade> tradeSource(StreamExecutionEnvironment env) {
		// This is a bit of a hack to use Thread.sleep() for sequencing but it works for our test purposes
		DataStream<Trade> trades = env.addSource(new SourceFunction<Trade>() {
			private volatile boolean running = true;

			@Override
			public void run(SourceContext<Trade> sc) throws Exception {
				Thread.sleep(1000);
				sc.collectWithTimestamp(new Trade(1000L, 0L, "trade-1000"), 1000);
				sc.emitWatermark(new Watermark(1000));
				Thread.sleep(3000);
				sc.collectWithTimestamp(new Trade(1200L, 0L, "trade-1200"), 1200);
				sc.emitWatermark(new Watermark(1200));
				Thread.sleep(1000);
				sc.collectWithTimestamp(new Trade(1500L, 0L, "trade-1500"), 1500);
				sc.emitWatermark(new Watermark(1500));
				Thread.sleep(1000);
				sc.collectWithTimestamp(new Trade(1700L, 0L, "trade-1700"), 1700);
				sc.emitWatermark(new Watermark(1700));
				Thread.sleep(1000);
				sc.collectWithTimestamp(new Trade(1800L, 0L, "trade-1800"), 1800);
				sc.emitWatermark(new Watermark(1800));
				Thread.sleep(1000);
				sc.collectWithTimestamp(new Trade(2000L, 0L, "trade-2000"), 2000);
				sc.emitWatermark(new Watermark(2000));


				while (running) {
					Thread.sleep(1000);
				}
			}

			@Override
			public void cancel() {
				running = false;
			}
		});

		return trades;
	}
}
