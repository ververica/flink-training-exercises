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

package com.dataArtisans.flinkTraining.exercises.dataStreamScala.rideCleansing

import com.dataArtisans.flinkTraining.exercises.dataStreamJava.dataTypes.TaxiRide
import com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils.{GeoUtils, TaxiRideGenerator, TaxiRideSchema}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.api.KafkaSink

/**
 *
 */
object RideCleansing {

  val LOCAL_KAFKA_BROKER = "localhost:9092"
  val CLEANSED_RIDES_TOPIC: String = "cleansedRides"

  def main(args: Array[String]) {

    // parse parameters
    val params = ParameterTool.fromArgs(args)
    val input = params.getRequired("input")
    val speed = params.getFloat("speed", 1.0f)

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // get the taxi ride data stream
    val rides = env.addSource(new TaxiRideGenerator(input, speed))

    val filteredRides = rides
      // filter out rides that do not start and end in NYC
      .filter(r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat))

    // write the filtered data to a Kafka sink
    filteredRides.addSink(
      new KafkaSink[TaxiRide](LOCAL_KAFKA_BROKER, CLEANSED_RIDES_TOPIC, new TaxiRideSchema))

    // run the cleansing pipeline
    env.execute("Taxi Ride Cleansing")
  }

}
