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

package com.ververica.flinktraining.exercises.datastream_java.windows;

import com.ververica.flinktraining.exercises.datastream_scala.windows.PopularPlacesExercise;
import org.apache.flink.api.java.tuple.Tuple5;

import java.util.ArrayList;
import java.util.List;

public class PopularPlacesScalaTest extends PopularPlacesTest {

	static Testable scalaExercise = () -> PopularPlacesExercise.main(new String[]{"-threshold", "2"});

	protected List<Tuple5<Float, Float, Long, Boolean, Integer>> results(TestRideSource source) throws Exception {
		Testable scalaSolution = () -> com.ververica.flinktraining.solutions.datastream_scala.windows.PopularPlacesSolution.main(new String[]{"-threshold", "2"});
		List<?> tuples = runApp(source, new TestSink<>(), scalaExercise, scalaSolution);
		return javaTuples((ArrayList<scala.Tuple5<Float, Float, Long, Boolean, Integer>>) tuples);
	}

	private ArrayList<Tuple5<Float, Float, Long, Boolean, Integer>> javaTuples(ArrayList<scala.Tuple5<Float, Float, Long, Boolean, Integer>> a) {
		ArrayList<Tuple5<Float, Float, Long, Boolean, Integer>> javaCopy = new ArrayList<>(a.size());
		a.iterator().forEachRemaining(t -> javaCopy.add(new Tuple5(t._1(), t._2(), t._3(), t._4(), t._5())));
		return javaCopy;
	}

}