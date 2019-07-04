/*
 * Copyright 2018 data Artisans GmbH, 2019 Ververica GmbH
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

package com.ververica.flinktraining.exercises.datastream_java.process;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.Customer;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.EnrichedTrade;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.Trade;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

public class EventTimeJoinTest {

	@Test
	public void testTradeBeforeCustomer() throws Exception {
		TwoInputStreamOperatorTestHarness<Trade, Customer, EnrichedTrade> testHarness = setupHarness();
		int timerCountBefore;

		// push in data
		Customer c0 = new Customer(0L, 0L, "customer-0");

		Trade t1000 = new Trade(1000L, 0L, "trade-1000");

		Customer c0500 = new Customer(500L, 0L, "customer-500");
		Customer c1500 = new Customer(1500L, 0L, "customer-1500");

		testHarness.processElement2(new StreamRecord<>(c0, 0));
		testHarness.processWatermark2(new Watermark(0));

		// processing a Trade should create an event-time timer
		timerCountBefore = testHarness.numEventTimeTimers();
		testHarness.processElement1(new StreamRecord<>(t1000, 1000));
		assert(testHarness.numEventTimeTimers() == timerCountBefore + 1);

		testHarness.processWatermark1(new Watermark(1000));

		testHarness.processElement2(new StreamRecord<>(c0500, 500));
		testHarness.processWatermark2(new Watermark(500));

		testHarness.processElement2(new StreamRecord<>(c1500, 1500));

		// now the timer should fire
		timerCountBefore = testHarness.numEventTimeTimers();
		testHarness.processWatermark2(new Watermark(1500));
		assert(testHarness.numEventTimeTimers() == timerCountBefore - 1);

		// verify operator state
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		EnrichedTrade et1000 = new EnrichedTrade(t1000, c0500);

		expectedOutput.add(new Watermark(0L));
		expectedOutput.add(new Watermark(500L));
		expectedOutput.add(new StreamRecord<>(et1000, 1000L));
		expectedOutput.add(new Watermark(1000L));

		ConcurrentLinkedQueue<Object> actualOutput = testHarness.getOutput();

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, actualOutput);

		testHarness.close();
	}

	@Test
	public void testTradeAfterCustomer() throws Exception {
		TwoInputStreamOperatorTestHarness<Trade, Customer, EnrichedTrade> testHarness = setupHarness();

		// push in data
		Customer c0500 = new Customer(500L, 0L, "customer-500");
		Customer c1500 = new Customer(1500L, 0L, "customer-1500");

		Trade t1200 = new Trade(1200L, 0L, "trade-1200");
		Trade t1500 = new Trade(1500L, 0L, "trade-1500");

		testHarness.processElement2(new StreamRecord<>(c0500, 500));
		testHarness.processWatermark2(new Watermark(500));

		testHarness.processElement2(new StreamRecord<>(c1500, 1500));
		testHarness.processWatermark2(new Watermark(1500));

		testHarness.processElement1(new StreamRecord<>(t1200, 1200));
		testHarness.processWatermark1(new Watermark(1200));

		testHarness.processElement1(new StreamRecord<>(t1500, 1500));
		testHarness.processWatermark1(new Watermark(1500));

		// verify operator state
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		EnrichedTrade et1200 = new EnrichedTrade(t1200, c0500);
		EnrichedTrade et1500 = new EnrichedTrade(t1500, c1500);

		expectedOutput.add(new StreamRecord<>(et1200, 1200L));
		expectedOutput.add(new Watermark(1200L));
		expectedOutput.add(new StreamRecord<>(et1500, 1500L));
		expectedOutput.add(new Watermark(1500L));

		ConcurrentLinkedQueue<Object> actualOutput = testHarness.getOutput();

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, actualOutput);

		testHarness.close();
	}

	@Test
	public void testManyPendingTrades() throws Exception {
		TwoInputStreamOperatorTestHarness<Trade, Customer, EnrichedTrade> testHarness = setupHarness();

		// push in data
		Trade t1700 = new Trade(1700L, 0L, "trade-1700");
		Trade t1800 = new Trade(1800L, 0L, "trade-1800");
		Trade t2000 = new Trade(2000L, 0L, "trade-2000");

		Customer c1600 = new Customer(1600L, 0L, "customer-1600");
		Customer c2100 = new Customer(2100L, 0L, "customer-2100");

		testHarness.processElement1(new StreamRecord<>(t1700, 1700));
		testHarness.processWatermark1(new Watermark(1700));

		testHarness.processElement1(new StreamRecord<>(t1800, 1800));
		testHarness.processWatermark1(new Watermark(1800));

		testHarness.processElement1(new StreamRecord<>(t2000, 2000));
		testHarness.processWatermark1(new Watermark(2000));

		testHarness.processElement2(new StreamRecord<>(c1600, 1600));
		testHarness.processWatermark2(new Watermark(1600));

		testHarness.processElement2(new StreamRecord<>(c2100, 2100));
		testHarness.processWatermark2(new Watermark(2100));

		// verify operator state
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		EnrichedTrade et1700 = new EnrichedTrade(t1700, c1600);
		EnrichedTrade et1800 = new EnrichedTrade(t1800, c1600);
		EnrichedTrade et2000 = new EnrichedTrade(t2000, c1600);

		expectedOutput.add(new Watermark(1600L));
		expectedOutput.add(new StreamRecord<>(et1700, 1700L));
		expectedOutput.add(new StreamRecord<>(et1800, 1800L));
		expectedOutput.add(new StreamRecord<>(et2000, 2000L));
		expectedOutput.add(new Watermark(2000L));

		ConcurrentLinkedQueue<Object> actualOutput = testHarness.getOutput();

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, actualOutput);

		testHarness.close();
	}

	private TwoInputStreamOperatorTestHarness<Trade, Customer, EnrichedTrade> setupHarness() throws Exception {
		// instantiate operator
		KeyedCoProcessOperator<Long, Trade, Customer, EnrichedTrade> operator =
				new KeyedCoProcessOperator<>(new EventTimeJoinExercise.EventTimeJoinFunction());

		// setup test harness
		TwoInputStreamOperatorTestHarness<Trade, Customer, EnrichedTrade> testHarness =
				new KeyedTwoInputStreamOperatorTestHarness<>(operator,
						(Trade t) -> t.customerId,
						(Customer c) -> c.customerId,
						BasicTypeInfo.LONG_TYPE_INFO);

		testHarness.setup();
		testHarness.open();

		return testHarness;
	}
}
