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

import java.net.{InetAddress, InetSocketAddress}
import java.util

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils
import org.apache.flink.api.common.functions.{RuntimeContext, MapFunction}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.elasticsearch2._
import org.apache.flink.util.Collector
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

import scala.collection.JavaConverters._

/**
  * Scala reference implementation for the "Popular Places" exercise of the Flink training
  * (http://training.data-artisans.com).
  *
  * The task of the exercise is to identify every five minutes popular areas where many taxi rides
  * arrived or departed in the last 15 minutes.
  * The results are written into an Elasticsearch index.
  *
  * Parameters:
  * -input path-to-input-file
  */
object PopularPlacesToES {

  def main(args: Array[String]) {

    // read parameters
    val params = ParameterTool.fromArgs(args)
    val input = params.getRequired("input")

    val popThreshold = 20 // threshold for popular places
    val maxDelay = 60     // events are out of order by max 60 seconds
    val speed = 600       // events of 10 minutes are served in 1 second

    // set up streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // start the data generator
    val rides = env.addSource(new TaxiRideSource(input, maxDelay, speed))

    // find popular places
    val popularPlaces = rides
      // remove all rides which are not within NYC
      .filter { r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat) }
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

    val config = Map(
      // This instructs the sink to emit after every element, otherwise they would be buffered
      "bulk.flush.max.actions" -> "10",
      // default cluster name
      "cluster.name" -> "elasticsearch"
    )
    val jConfig: java.util.Map[String, String] = new java.util.HashMap()
    jConfig.putAll(config.asJava)

    val transports = List(new InetSocketAddress(InetAddress.getByName("localhost"), 9300))
    val jTransports = new util.ArrayList(transports.asJava)

    popularPlaces.addSink(
      new ElasticsearchSink(jConfig, jTransports, new PopularPlaceInserter))

    // execute the transformation pipeline
    env.execute("Popular Places to Elasticsearch")
  }

  /**
    * Inserts popular places into the "nyc-places" index.
    */
  class PopularPlaceInserter extends ElasticsearchSinkFunction[(Float, Float, Long, Boolean, Int)] {

    def process(record: (Float, Float, Long, Boolean, Int), ctx: RuntimeContext, indexer: RequestIndexer) {

      val json = Map(
        "time" -> record._3.toString,
        "location" -> (record._2 + "," + record._1),
        "isStart" -> record._4.toString,
        "cnt" -> record._5.toString
      )

      val rqst: IndexRequest = Requests.indexRequest
        .index("nyc-places")
        .`type`("popular-locations")
        .source(json.asJava)

      indexer.add(rqst)
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
