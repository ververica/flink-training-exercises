/*
 * Copyright 2017 data Artisans GmbH
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

package com.dataartisans.flinktraining.exercises.datastream_scala.cep

import java.util.{List => JList, Map => JMap}

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide
import com.dataartisans.flinktraining.exercises.datastream_java.sources.CheckpointedTaxiRideSource
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.{PatternFlatSelectFunction, PatternFlatTimeoutFunction}
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

/**
  * Scala/CEP reference implementation for the "Long Ride Alerts" exercise of the Flink training
  * (http://training.data-artisans.com).
  *
  * The goal for this exercise is to emit START events for taxi rides that have not been matched
  * by an END event during the first 2 hours of the ride.
  *
  * Parameters:
  * -input path-to-input-file
  *
  */
object LongRides {
  def main(args: Array[String]) {
    // parse parameters
    val params = ParameterTool.fromArgs(args)
    val input = params.getRequired("input")

    val speed = 600   // events of 10 minutes are served in 1 second

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // operate in Event-time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // get the taxi ride data stream, in order
    val rides = env.addSource(new CheckpointedTaxiRideSource(input, speed))

    val keyedRides = rides.keyBy(_.rideId)

    // A complete taxi ride has a START event followed by an END event
    val completedRides = Pattern
      .begin[TaxiRide]("start").where(_.isStart)
      .next("end").where(!_.isStart)

    // We want to find rides that have NOT been completed within 120 minutes
    CEP.pattern[TaxiRide](keyedRides, completedRides.within(Time.minutes(120)))
      .flatSelect[TaxiRide, TaxiRide](
        new TaxiRideTimedOut[TaxiRide],
        new FlatSelectNothing[TaxiRide])
      .print()

    env.execute("Long Taxi Rides")
  }

  class TaxiRideTimedOut[TaxiRide] extends PatternFlatTimeoutFunction[TaxiRide, TaxiRide] {
    override def timeout(map: JMap[String, JList[TaxiRide]], l: Long, collector: Collector[TaxiRide]): Unit = {
      val rideStarted = map.get("start").get(0)
      collector.collect(rideStarted)
    }
  }

  class FlatSelectNothing[T] extends PatternFlatSelectFunction[T, T] {
    override def flatSelect(pattern: JMap[String, JList[T]], collector: Collector[T]): Unit = {
    }
  }

}
