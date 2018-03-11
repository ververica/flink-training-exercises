/*
 * Copyright 2018 data Artisans GmbH
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

package com.dataartisans.flinktraining.exercises.datastream_scala.windows

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiFare
import com.dataartisans.flinktraining.exercises.datastream_java.sources.CheckpointedTaxiFareSource
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * Scala reference implementation for the "Popular Places" exercise of the Flink training
 * (http://training.data-artisans.com).
 *
 * The task of the exercise is to identify every five minutes popular areas where many taxi rides
 * arrived or departed in the last 15 minutes.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
object HourlyTips {

  def main(args: Array[String]) {

    // read parameters
    val params = ParameterTool.fromArgs(args)
    val input = params.getRequired("input")

    val speed = 600 // events of 10 minutes are served in 1 second

    // set up streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // start the data generator
    val fares = env.addSource(new CheckpointedTaxiFareSource(input, speed))

    // total tips per hour by driver
    val hourlyTips = fares
      .keyBy(fare => fare.driverId)
      .timeWindow(Time.hours(1))
      .apply { (key: Long, window, fares, out: Collector[(Long, Long, Float)]) =>
        out.collect((window.getEnd, key, fares.map(fare => fare.tip).sum))
      }

    // max tip total in each hour
    val hourlyMax = hourlyTips
      .timeWindowAll(Time.hours(1))
      .maxBy(2)

    // print result on stdout
    hourlyMax.print()

    // execute the transformation pipeline
    env.execute("Hourly Tips (scala)")
  }

}
