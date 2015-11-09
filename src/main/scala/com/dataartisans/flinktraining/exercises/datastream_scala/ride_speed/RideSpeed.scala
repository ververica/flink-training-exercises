/*
 * Copyright 2015 data Artisans GmbH
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

package com.dataartisans.flinktraining.exercises.datastream_scala.ride_speed

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.OperatorState
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * Scala reference implementation for the "Ride Speed" exercise of the Flink training
 * (http://dataartisans.github.io/flink-training).
 *
 * The task of the exercise is to compute the average speed of completed taxi rides from a data
 * stream of taxi ride records.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
object RideSpeed {

  @throws(classOf[Exception])
  def main(args: Array[String]) {

    // parse parameters
    val params = ParameterTool.fromArgs(args)
    val input = params.getRequired("input")
    val speed = params.getFloat("speed", 1.0f)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // get the taxi ride data stream
    val rides = env.addSource(new TaxiRideSource(input, speed))

    val rideSpeeds = rides
      // filter out rides that do not start and end in NYC
      .filter(r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat))
      // group records by rideId
      .keyBy("rideId")
      // compute the average speed of a ride
      .flatMap(new SpeedComputer)

    // print the result to stdout
    rideSpeeds.print()

    // run the transformation pipeline
    env.execute("Average Ride Speed")
  }

  /**
   * Computes the average speed of a taxi ride.
   */
  class SpeedComputer extends RichFlatMapFunction[TaxiRide, (Long, Float)] {

    var state: OperatorState[TaxiRide] = null

    override def open(config: Configuration): Unit = {
      state = getRuntimeContext.getKeyValueState("ride", classOf[TaxiRide], null)
    }

    override def flatMap(ride: TaxiRide, out: Collector[(Long, Float)]): Unit = {

      if(state.value() == null) {
        // first ride
        state.update(ride)
      }
      else {
        // second ride
        val startEvent = if (ride.isStart) ride else state.value()
        val endEvent = if (ride.isStart) state.value() else ride

        val timeDiff = endEvent.time.getMillis - startEvent.time.getMillis
        val speed = if (timeDiff != 0) {
          (endEvent.travelDistance / timeDiff) * 60 * 60 * 1000
        } else {
          -1
        }
        // emit average speed
        out.collect( (startEvent.rideId, speed) )

        // clear state to free memory
        state.update(null)
      }
    }
  }

}

