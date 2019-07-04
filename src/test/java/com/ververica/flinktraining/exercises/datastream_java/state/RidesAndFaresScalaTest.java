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

package com.ververica.flinktraining.exercises.datastream_java.state;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_scala.state.RidesAndFaresExercise;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class RidesAndFaresScalaTest extends RidesAndFaresTest {

	static Testable scalaExercise = () -> RidesAndFaresExercise.main(new String[]{});

	protected List<?> results(TestRideSource rides, TestFareSource fares) throws Exception {
		Testable scalaSolution = () -> com.ververica.flinktraining.solutions.datastream_scala.state.RidesAndFaresSolution.main(new String[]{});
		List<?> tuples = runApp(rides, fares, new TestSink<>(), scalaExercise, scalaSolution);
		return javaTuples((ArrayList<scala.Tuple2<TaxiRide, TaxiFare>>) tuples);
	}

	private ArrayList<Tuple2<TaxiRide, TaxiFare>> javaTuples(ArrayList<scala.Tuple2<TaxiRide, TaxiFare>> a) {
		ArrayList<Tuple2<TaxiRide, TaxiFare>> javaCopy = new ArrayList<>(a.size());
		a.iterator().forEachRemaining(t -> javaCopy.add(new Tuple2(t._1(), t._2())));
		return javaCopy;
	}
}