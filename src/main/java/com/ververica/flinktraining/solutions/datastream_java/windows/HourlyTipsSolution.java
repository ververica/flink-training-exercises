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

package com.ververica.flinktraining.solutions.datastream_java.windows;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Java reference implementation for the "Hourly Tips" exercise of the Flink training
 * (http://training.ververica.com).
 *
 * The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
 * then from that stream, find the highest tip total in each hour.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
public class HourlyTipsSolution extends ExerciseBase {

	public static void main(String[] args) throws Exception {

		// read parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToFareData);

		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		Configuration conf = new Configuration();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		// start the data generator
		DataStream<TaxiFare> fares = env.addSource(fareSourceOrTest(new TaxiFareSource(input, maxEventDelay, servingSpeedFactor))).name("testname");

		// compute tips per hour for each driver
		fares
				.keyBy((TaxiFare fare) -> fare.driverId)
				.timeWindow(Time.hours(1))
				.process(new AddTips())
				.timeWindowAll(Time.hours(1))
				.maxBy(2)
				.print();

//		printOrTest(hourlyMax);

		System.out.println(env.getExecutionPlan());

		// execute the transformation pipeline
		env.execute("Hourly Tips (java)");
	}

	/*
	 * Wraps the pre-aggregated result into a tuple along with the window's timestamp and key.
	 */
	public static class AddTips extends ProcessWindowFunction<
			TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {
		@Override
		public void process(Long key, Context context, Iterable<TaxiFare> fares, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
			Float sumOfTips = 0F;
			for (TaxiFare f : fares) {
				sumOfTips += f.tip;
			}
			out.collect(new Tuple3<>(context.window().getEnd(), key, sumOfTips));
		}
	}
}
