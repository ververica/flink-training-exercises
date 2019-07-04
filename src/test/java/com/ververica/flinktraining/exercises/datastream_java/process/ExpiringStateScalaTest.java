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

import com.ververica.flinktraining.exercises.datastream_scala.process.ExpiringStateExercise;
import java.util.List;

public class ExpiringStateScalaTest extends ExpiringStateTest {

	static Testable scalaExercise = () -> ExpiringStateExercise.main(new String[]{});

	protected List<?> results(TestRideSource rides, TestFareSource fares) throws Exception {
		Testable scalaSolution = () -> com.ververica.flinktraining.solutions.datastream_scala.process.ExpiringStateSolution.main(new String[]{});
		return runApp(rides, fares, new TestSink<>(), scalaExercise, scalaSolution);
	}

}