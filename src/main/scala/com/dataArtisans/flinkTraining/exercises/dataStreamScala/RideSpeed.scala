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

package com.dataArtisans.flinkTraining.exercises.dataStreamScala

import com.dataArtisans.flinkTraining.exercises.dataStreamJava.dataTypes.TaxiRide
import com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils.TaxiRideSchema
import org.apache.flink.api.common.functions.{MapFunction, FlatMapFunction}
import org.apache.flink.util.Collector

import scala.collection.mutable

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.api.KafkaSource


object RideSpeed {
  private val LOCAL_ZOOKEEPER_HOST: String = "localhost:2181"

  @throws(classOf[Exception])
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val rides = env.addSource(
      new KafkaSource[TaxiRide](
        LOCAL_ZOOKEEPER_HOST,
        RideCleansing.CLEANSED_RIDES_TOPIC,
        new TaxiRideSchema))

    val rideSpeeds = rides
      .groupBy("rideId")
      .flatMap(new RideEventJoiner)
      .map(new SpeedComputer)

    rideSpeeds.print()

    env.execute("Average Ride Speed")
  }

  class RideEventJoiner extends FlatMapFunction[TaxiRide, (TaxiRide, TaxiRide)] {

    private val startRecords = mutable.HashMap.empty[Long, TaxiRide]

    def flatMap(rideEvent: TaxiRide, out: Collector[(TaxiRide, TaxiRide)]) {
      if (rideEvent.isStart) {
        startRecords += (rideEvent.rideId -> rideEvent)
      } else {
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

