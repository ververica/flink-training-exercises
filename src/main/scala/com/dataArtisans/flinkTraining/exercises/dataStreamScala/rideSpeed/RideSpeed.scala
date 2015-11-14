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

package com.dataArtisans.flinkTraining.exercises.dataStreamScala.rideSpeed

import com.dataArtisans.flinkTraining.exercises.dataStreamJava.dataTypes.TaxiRide
import com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils.{GeoUtils, TaxiRideGenerator }
import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
 * Scala reference implementation for the "Ride Speed" exercise of the Flink training (http://dataartisans.github.io/flink-training).
 * The task of the exercise is to read taxi ride records from an Apache Kafka topic and compute the average speed of completed taxi rides.
 *
 * Parameters:
 * --input path-to-input-directory
 * --speed serving-speed-of-generator
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

    // get the taxi ride data stream
    val rides = env.addSource(new TaxiRideGenerator(input, speed))

    val rideSpeeds = rides
      // filter out rides that do not start and end in NYC
      .filter(r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat))
      // group records by rideId
      .keyBy("rideId")
      // match ride start and end records
      .flatMap(new RideEventJoiner)
      // compute the average speed of a ride
      .map(new SpeedComputer)

    // emit the result on stdout
    rideSpeeds.print()

    // run the transformation pipeline
    env.execute("Average Ride Speed")
  }

  /**
   * Matches start and end TaxiRide records.
   */
  class RideEventJoiner extends FlatMapFunction[TaxiRide, (TaxiRide, TaxiRide)] {

    private val startRecords = mutable.HashMap.empty[Long, TaxiRide]

    def flatMap(rideEvent: TaxiRide, out: Collector[(TaxiRide, TaxiRide)]) {
      if (rideEvent.isStart) {
        // remember start record
        startRecords += (rideEvent.rideId -> rideEvent)
      } else {
        // get and forget start record
        startRecords.remove(rideEvent.rideId) match {
          case Some(startRecord) => out.collect((startRecord, rideEvent))
          case _ => // we have no start record, ignore this one
        }
      }
    }
  }

  object SpeedComputer {
    private var MILLIS_PER_HOUR: Int = 1000 * 60 * 60
  }

  /**
   * Computes the average speed of a taxi ride from its start and end record.
   */
  class SpeedComputer extends MapFunction[(TaxiRide, TaxiRide), (Long, Float)] {

    def map(joinedEvents: (TaxiRide, TaxiRide)): (Long, Float) = {
      val startTime = joinedEvents._1.time.getMillis
      val endTime = joinedEvents._2.time.getMillis
      val distance = joinedEvents._2.travelDistance

      val timeDiff = endTime - startTime
      val speed = if (timeDiff != 0) {
        (distance / timeDiff) * SpeedComputer.MILLIS_PER_HOUR
      } else {
        -1
      }

      (joinedEvents._1.rideId, speed)
    }
  }

}

