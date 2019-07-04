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

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.testing.TaxiRideTestBase;

import java.util.List;

public class LongRidesScalaTest extends LongRidesTest {

	static Testable scalaExercise = () -> com.ververica.flinktraining.exercises.datastream_scala.process.LongRidesExercise.main(new String[]{});
	static Testable scalaCEPExercise = () -> com.ververica.flinktraining.exercises.datastream_scala.cep.LongRidesExercise.main(new String[]{});

	protected List<TaxiRide> results(TestRideSource source) throws Exception {
		Testable scalaSolution = () -> com.ververica.flinktraining.solutions.datastream_scala.process.LongRidesSolution.main(new String[]{});
		return runApp(source, new TestSink<>(), scalaExercise, scalaSolution);
	}

	protected List<TaxiRide> cepResults(TestRideSource source) throws Exception {
		Testable scalaCEPSolution = () -> com.ververica.flinktraining.solutions.datastream_scala.cep.LongRidesSolution.main(new String[]{});
		return runApp(source, new TestSink<>(), scalaCEPExercise, scalaCEPSolution);
	}

	protected List<TaxiRide> checkpointedResults(TestRideSource source) throws Exception {
		Testable scalaCheckpointedSolution = () -> com.ververica.flinktraining.solutions.datastream_scala.process.CheckpointedLongRidesSolution.main(new String[]{});
		return runApp(source, new TestSink<>(), scalaCheckpointedSolution);
	}

}