/*
 * Copyright 2018 data Artisans GmbH, 2019 Ververica GmbH
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

package com.ververica.flinktraining.exercises.datastream_scala.broadcast

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource
import com.ververica.flinktraining.exercises.datastream_java.utils.{ExerciseBase, MissingSolutionException}
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase.{printOrTest, rideSourceOrTest}
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * The "Nearest Future Taxi" exercise of the Flink training
 * (http://training.ververica.com).
 *
 * Given a location that is broadcast, the goal of this exercise is to watch the stream of
 * taxi rides and report on taxis that complete rides closest to the requested location.
 * The application should be able to handle simultaneous queries.
 *
 * Parameters:
 * -input path-to-input-file
 *
 * Use
 *
 *     nc -lk 9999
 *
 * (or nc -l -p 9999, depending on your version of netcat)
 * to establish a socket stream from stdin on port 9999.
 * On Windows you can use ncat from https://nmap.org/ncat/.
 *
 * Some good locations:
 *
 * -74, 41 					(Near, but outside the city to the NNW)
 * -73.7781, 40.6413 			(JFK Airport)
 * -73.977664, 40.761484		(Museum of Modern Art)
 */

case class Query(queryId: Long, longitude: Float, latitude: Float)

object Query {
  def apply(longitude: Float, latitude: Float): Query = new Query(Random.nextLong, longitude, latitude)
}

object NearestTaxiExercise {

  val queryDescriptor = new MapStateDescriptor[Long, Query]("queries",
    createTypeInformation[Long], createTypeInformation[Query])

  def main(args: Array[String]): Unit = {
    // parse parameters
    val params = ParameterTool.fromArgs(args)
    val ridesFile = params.get("input", ExerciseBase.pathToRideData)

    val maxEventDelay = 60        // events are out of order by at most 60 seconds
    val servingSpeedFactor = 600  // 10 minutes worth of events are served every second

    // set up streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(ExerciseBase.parallelism)

    val rides = env.addSource(new TaxiRideSource(ridesFile, maxEventDelay, servingSpeedFactor))

    val splitQuery = (msg: String) => {
      val parts: Array[Float] = msg.split(",\\s*").map(_.toFloat)
      Query(parts(0), parts(1))
    }
    // add a socket source
    val queryStream = env.socketTextStream("localhost", 9999)
      .map(splitQuery)
      .broadcast(queryDescriptor)

    val reports = rides
      .keyBy(_.taxiId)
      .connect(queryStream)
      .process(new QueryFunction)

    val nearest = reports
      // key by the queryId
      .keyBy(_._1)
      // the minimum, for each query, by distance
      .minBy(2)

    nearest.print()

    env.execute("Nearest Available Taxi")
  }

  // Note that in order to have consistent results after a restore from a checkpoint, the
  // behavior of this method must be deterministic, and NOT depend on characteristics of an
  // individual sub-task.
  class QueryFunction extends KeyedBroadcastProcessFunction[Long, TaxiRide, Query, (Long, Long, Float)]{

    override def processElement(ride: TaxiRide,
                                readOnlyContext: KeyedBroadcastProcessFunction[Long, TaxiRide, Query, (Long, Long, Float)]#ReadOnlyContext,
                                out: Collector[(Long, Long, Float)]): Unit =
      if (!ride.isStart) {
        throw new MissingSolutionException
      }

    override def processBroadcastElement(query: Query,
                                         context: KeyedBroadcastProcessFunction[Long, TaxiRide, Query, (Long, Long, Float)]#Context,
                                         out: Collector[(Long, Long, Float)]): Unit = {
      println("New query: " + query)
      context.getBroadcastState(queryDescriptor).put(query.queryId, query)
    }
  }

}
