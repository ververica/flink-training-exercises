/*
 * Copyright 2019 Ververica GmbH
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

package com.ververica.flinktraining.solutions.datastream_java.state_processor;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.util.Collector;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;

import static org.apache.flink.api.java.aggregation.Aggregations.MIN;
import static org.apache.flink.api.java.aggregation.Aggregations.SUM;

public class ReadRidesAndFaresSnapshot {
	static class ReadRidesAndFares extends KeyedStateReaderFunction<Long, Tuple2<TaxiRide, TaxiFare>> {
		ValueState<TaxiRide> ride;
		ValueState<TaxiFare> fare;

		@Override
		public void open(Configuration parameters) {
			ride = getRuntimeContext().getState(new ValueStateDescriptor<>("saved ride", TaxiRide.class));
			fare = getRuntimeContext().getState(new ValueStateDescriptor<>("saved fare", TaxiFare.class));
		}

		@Override
		public void readKey(
				Long key,
				Context context,
				Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {

			out.collect(new Tuple2(ride.value(), fare.value()));
		}
	}

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();
		MemoryStateBackend backend = new MemoryStateBackend();

		/***************************************************************************************
		 Update this path to point to a checkpoint or savepoint from RidesAndFaresSolution.java
		 ***************************************************************************************/
		String pathToSnapshot = "file:///tmp/checkpoints/d026157cec94402dd98c6be51d4db8ca/chk-2";

		ExistingSavepoint sp = Savepoint.load(bEnv, pathToSnapshot, backend);

		DataSet<Tuple2<TaxiRide, TaxiFare>> keyedState = sp.readKeyedState(
				"enrichment",
				new ReadRidesAndFares());

		keyedState.print();
	}
}
