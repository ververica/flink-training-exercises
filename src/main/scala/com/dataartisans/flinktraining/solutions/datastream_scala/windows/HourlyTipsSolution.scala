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

package com.dataartisans.flinktraining.solutions.datastream_scala.windows

import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiFareSource
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Scala reference implementation for the "Hourly Tips" exercise of the Flink training
  * (http://training.data-artisans.com).
  *
  * The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
  * then from that stream, find the highest tip total in each hour.
  *
  * Parameters:
  * -input path-to-input-file
  *
  */
object HourlyTipsSolution {

  def main(args: Array[String]) {

    // read parameters
    val params = ParameterTool.fromArgs(args)
    val input = params.get("input", ExerciseBase.pathToFareData)

    val maxDelay = 60 // events are delayed by at most 60 seconds
    val speed = 600   // events of 10 minutes are served in 1 second

    // set up streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(ExerciseBase.parallelism)

    // start the data generator
    val fares = env.addSource(fareSourceOrTest(new TaxiFareSource(input, maxDelay, speed)))

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
    printOrTest(hourlyMax)

    // execute the transformation pipeline
    env.execute("Hourly Tips (scala)")
  }

}
