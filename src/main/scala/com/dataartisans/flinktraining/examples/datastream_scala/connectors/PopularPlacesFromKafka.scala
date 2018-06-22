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

package com.dataartisans.flinktraining.examples.datastream_scala.connectors

import java.util.Properties

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide
import com.dataartisans.flinktraining.exercises.datastream_java.utils.{GeoUtils, TaxiRideSchema}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector

/**
 * Scala reference implementation for the "Popular Places" exercise of the Flink training
 * (http://training.data-artisans.com).
 *
 * The task of the exercise is to read taxi ride records from an Apache Kafka topic and to identify
 * every five minutes popular areas where many taxi rides arrived or departed in the last 15 minutes.
 * The input is read from a Kafka topic.
 *
 */
object PopularPlacesFromKafka {

  private val LOCAL_ZOOKEEPER_HOST = "localhost:2181"
  private val LOCAL_KAFKA_BROKER = "localhost:9092"
  private val RIDE_SPEED_GROUP = "rideSpeedGroup"
  private val MAX_EVENT_DELAY = 60 // events are out of order by max 60 seconds

  def main(args: Array[String]) {

    val popThreshold = 20 // threshold for popular places

    // set up streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // configure Kafka consumer
    val kafkaProps = new Properties
    kafkaProps.setProperty("zookeeper.connect", LOCAL_ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers", LOCAL_KAFKA_BROKER)
    kafkaProps.setProperty("group.id", RIDE_SPEED_GROUP)
    // always read the Kafka topic from the start
    kafkaProps.setProperty("auto.offset.reset", "earliest")

    // create a Kafka consumer
    val consumer = new FlinkKafkaConsumer011[TaxiRide](
        RideCleansingToKafka.CLEANSED_RIDES_TOPIC,
        new TaxiRideSchema,
        kafkaProps)
    // configure timestamp and watermark assigner
    consumer.assignTimestampsAndWatermarks(new TaxiRideTSAssigner)

    // create a Kafka source
    val rides = env.addSource(consumer)

    // find popular places
    val popularSpots = rides
      // match ride to grid cell and event type (start or end)
      .map(new GridCellMatcher)
      // partition by cell id and event type
      .keyBy( k => k )
      // build sliding window
      .timeWindow(Time.minutes(15), Time.minutes(5))
      // count events in window
      .apply{ (key: (Int, Boolean), window, vals, out: Collector[(Int, Long, Boolean, Int)]) =>
        out.collect( (key._1, window.getEnd, key._2, vals.size) )
      }
      // filter by popularity threshold
      .filter( c => { c._4 >= popThreshold } )
      // map grid cell to coordinates
      .map(new GridToCoordinates)

    // print result on stdout
    popularSpots.print()

    // execute the transformation pipeline
    env.execute("Popular Places from Kafka")
  }

  /**
   * Assigns timestamps to TaxiRide records.
   * Watermarks are periodically assigned, a fixed time interval behind the max timestamp.
   */
  class TaxiRideTSAssigner
      extends BoundedOutOfOrdernessTimestampExtractor[TaxiRide](Time.seconds(MAX_EVENT_DELAY)) {

    override def extractTimestamp(ride: TaxiRide): Long = {
      if(ride.isStart) ride.startTime.getMillis else ride.endTime.getMillis
    }
  }

  /**
   * Map taxi ride to grid cell and event type.
   * Start records use departure location, end record use arrival location.
   */
  class GridCellMatcher extends MapFunction[TaxiRide, (Int, Boolean)] {

    def map(taxiRide: TaxiRide): (Int, Boolean) = {
      if (taxiRide.isStart) {
        // get grid cell id for start location
        val gridId: Int = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat)
        (gridId, true)
      } else {
        // get grid cell id for end location
        val gridId: Int = GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat)
        (gridId, false)
      }
    }
  }

  /**
   * Maps the grid cell id back to longitude and latitude coordinates.
   */
  class GridToCoordinates extends MapFunction[
    (Int, Long, Boolean, Int),
    (Float, Float, Long, Boolean, Int)] {

    def map(cellCount: (Int, Long, Boolean, Int)): (Float, Float, Long, Boolean, Int) = {
      val longitude = GeoUtils.getGridCellCenterLon(cellCount._1)
      val latitude = GeoUtils.getGridCellCenterLat(cellCount._1)
      (longitude, latitude, cellCount._2, cellCount._3, cellCount._4)
    }
  }

}

