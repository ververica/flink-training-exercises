/*
 * Copyright 2015 data Artisans GmbH, 2019 Ververica GmbH
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
package com.ververica.flinktraining.examples.table_scala.stream.popularPlaces

import com.ververica.flinktraining.exercises.datastream_java.utils.GeoUtils.{IsInNYC, ToCellId, ToCoords}
import com.ververica.flinktraining.examples.table_java.sources.TaxiRideTableSource
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.types.Row
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Slide
import org.apache.flink.table.api.scala._

object PopularPlacesTableApi {

  def main(args: Array[String]) {

    // read parameters
    val params = ParameterTool.fromArgs(args)
    val input = params.getRequired("input")

    val maxEventDelay = 60       // events are out of order by max 60 seconds
    val servingSpeedFactor = 600 // events of 10 minutes are served in 1 second

    // set up streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // create TableEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    // register TaxiRideTableSource as table "TaxiRides"
    tEnv.registerTableSource(
      "TaxiRides",
      new TaxiRideTableSource(input, maxEventDelay, servingSpeedFactor))

    // create user-defined functions
    val isInNYC = new IsInNYC
    val toCellId = new ToCellId
    val toCoords = new ToCoords

    val popPlaces = tEnv
      // scan TaxiRides table
      .scan("TaxiRides")
      // filter for valid rides
      .filter(isInNYC('startLon, 'startLat) && isInNYC('endLon, 'endLat))
      // select fields and compute grid cell of departure or arrival coordinates
      .select(
        'eventTime,
        'isStart,
        'isStart.?(toCellId('startLon, 'startLat), toCellId('endLon, 'endLat)) as 'cell)
      // specify sliding window of 15 minutes with slide of 5 minutes
      .window(Slide over 15.minutes every 5.minutes on 'eventTime as 'w)
      // group by cell, isStart, and window
      .groupBy('cell, 'isStart, 'w)
      // count departures and arrivals per cell (location) and window (time)
      .select('cell, 'isStart, 'w.start as 'start, 'w.end as 'end, 'isStart.count as 'popCnt)
      // filter for popular places
      .filter('popCnt > 20)
      // convert cell back to coordinates
      .select(toCoords('cell) as 'location, 'start, 'end, 'isStart, 'popCnt)

    // convert Table into an append stream and print it
    tEnv.toAppendStream[Row](popPlaces).print

    // execute query
    env.execute
  }

}
