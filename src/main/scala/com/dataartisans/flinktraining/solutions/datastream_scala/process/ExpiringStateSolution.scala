/*
 * Copyright 2017 data Artisans GmbH
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

package com.dataartisans.flinktraining.solutions.datastream_scala.process

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.{TaxiFare, TaxiRide}
import com.dataartisans.flinktraining.exercises.datastream_java.sources.{CheckpointedTaxiFareSource, CheckpointedTaxiRideSource}
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

/**
  * Scala reference implementation for the "Expiring State" exercise of the Flink training
  * (http://training.data-artisans.com).
  *
  * The goal for this exercise is to enrich TaxiRides with fare information.
  *
  * Parameters:
  * -rides path-to-input-file
  * -fares path-to-input-file
  *
  */
object ExpiringStateSolution {
  val unmatchedRides = new OutputTag[TaxiRide]("unmatchedRides") {}
  val unmatchedFares = new OutputTag[TaxiFare]("unmatchedFares") {}

  def main(args: Array[String]) {

    // parse parameters
    val params = ParameterTool.fromArgs(args)
    val ridesFile = params.get("rides", ExerciseBase.pathToRideData)
    val faresFile = params.get("fares", ExerciseBase.pathToFareData)

    val servingSpeedFactor = 600 // 10 minutes worth of events are served every second

    // set up streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(ExerciseBase.parallelism)

    val rides = env
      .addSource(rideSourceOrTest(new CheckpointedTaxiRideSource(ridesFile, servingSpeedFactor)))
      .filter { ride => ride.isStart && (ride.rideId % 1000 != 0) }
      .keyBy("rideId")

    val fares = env
      .addSource(fareSourceOrTest(new CheckpointedTaxiFareSource(faresFile, servingSpeedFactor)))
      .keyBy("rideId")

    val processed = rides.connect(fares).process(new EnrichmentFunction)

    printOrTest(processed.getSideOutput[TaxiFare](unmatchedFares))

    env.execute("ExpiringState (scala)")
  }

  class EnrichmentFunction extends CoProcessFunction[TaxiRide, TaxiFare, (TaxiRide, TaxiFare)] {
    // keyed, managed state
    lazy val rideState: ValueState[TaxiRide] = getRuntimeContext.getState(
      new ValueStateDescriptor[TaxiRide]("saved ride", classOf[TaxiRide]))
    lazy val fareState: ValueState[TaxiFare] = getRuntimeContext.getState(
      new ValueStateDescriptor[TaxiFare]("saved fare", classOf[TaxiFare]))

    override def processElement1(ride: TaxiRide,
                                 context: CoProcessFunction[TaxiRide, TaxiFare, (TaxiRide, TaxiFare)]#Context,
                                 out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      val fare = fareState.value
      if (fare != null) {
        fareState.clear()
        out.collect((ride, fare))
      }
      else {
        rideState.update(ride)
        // as soon as the watermark arrives, we can stop waiting for the corresponding fare
        context.timerService.registerEventTimeTimer(ride.getEventTime)
      }
    }

    override def processElement2(fare: TaxiFare,
                                 context: CoProcessFunction[TaxiRide, TaxiFare, (TaxiRide, TaxiFare)]#Context,
                                 out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      val ride = rideState.value
      if (ride != null) {
        rideState.clear()
        out.collect((ride, fare))
      }
      else {
        fareState.update(fare)
        // as soon as the watermark arrives, we can stop waiting for the corresponding ride
        context.timerService.registerEventTimeTimer(fare.getEventTime)
      }
    }

    override def onTimer(timestamp: Long,
                         ctx: CoProcessFunction[TaxiRide, TaxiFare, (TaxiRide, TaxiFare)]#OnTimerContext,
                         out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      if (fareState.value != null) {
        ctx.output(unmatchedFares, fareState.value)
        fareState.clear()
      }
      if (rideState.value != null) {
        ctx.output(unmatchedRides, rideState.value)
        rideState.clear()
      }
    }
  }

}
