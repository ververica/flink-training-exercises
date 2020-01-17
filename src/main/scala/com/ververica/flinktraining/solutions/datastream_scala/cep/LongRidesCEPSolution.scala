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

package com.ververica.flinktraining.solutions.datastream_scala.cep

import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase._
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide
import com.ververica.flinktraining.exercises.datastream_java.sources.{CheckpointedTaxiRideSource, TaxiRideSource}
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.Map

/**
  * Scala/CEP reference implementation for the "Long Ride Alerts" exercise of the Flink training
  * (http://training.ververica.com).
  *
  * The goal for this exercise is to emit START events for taxi rides that have not been matched
  * by an END event during the first 2 hours of the ride.
  *
  * Parameters:
  * -input path-to-input-file
  *
  */
object LongRidesCEPSolution {
  def main(args: Array[String]) {
    val params = ParameterTool.fromArgs(args)
    val input = params.get("input", pathToRideData)

    val speed = 600   // events of 10 minutes are served in 1 second

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // operate in Event-time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(ExerciseBase.parallelism)

    // get the taxi ride data stream, in order
    val rides = env.addSource(rideSourceOrTest(new CheckpointedTaxiRideSource(input, speed)))

    val keyedRides = rides.keyBy(_.rideId)

    // A complete taxi ride has a START event followed by an END event
    val completedRides = Pattern
      .begin[TaxiRide]("start").where(_.isStart)
      .next("end").where(!_.isStart)

    // We want to find rides that have NOT been completed within 120 minutes
    // This pattern matches rides that ARE completed.
    // Below we will ignore rides that match this pattern, and emit those that timeout.
    val pattern: PatternStream[TaxiRide] = CEP.pattern[TaxiRide](keyedRides, completedRides.within(Time.minutes(120)))

    // side output tag for rides that time out
    val timedoutTag = new OutputTag[TaxiRide]("timedout")

    // collect rides that timeout
    val timeoutFunction = (map: Map[String, Iterable[TaxiRide]], timestamp: Long, out: Collector[TaxiRide]) => {
      val rideStarted = map.get("start").get.head
      out.collect(rideStarted)
    }

    // ignore rides that complete on time
    val selectFunction = (map: Map[String, Iterable[TaxiRide]], out: Collector[TaxiRide]) => {
    }

    val longRides = pattern.flatSelect(timedoutTag)(timeoutFunction)(selectFunction)

    printOrTest(longRides.getSideOutput(timedoutTag))

    env.execute("Long Taxi Rides (CEP)")
  }
}
