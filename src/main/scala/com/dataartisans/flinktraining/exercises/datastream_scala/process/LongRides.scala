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

package com.dataartisans.flinktraining.exercises.datastream_scala.process

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide
import com.dataartisans.flinktraining.exercises.datastream_java.sources.CheckpointedTaxiRideSource

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.{TimeCharacteristic, TimerService}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object LongRides {
  def main(args: Array[String]) {

    // parse parameters
    val params = ParameterTool.fromArgs(args)
    val input = params.getRequired("input")

    val speed = 600   // events of 10 minutes are served in 1 second

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // operate in Event-time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // get the taxi ride data stream, in order
    val rides = env.addSource(new CheckpointedTaxiRideSource(input, speed))

    val longRides = rides
      .keyBy(_.rideId)
      .process(new MatchFunction())

    longRides.print()

    env.execute("Long Taxi Rides")
  }

  class MatchFunction extends ProcessFunction[TaxiRide, TaxiRide] {
    // keyed, managed state -- matching START and END taxi ride events
    lazy val rideStartedState: ValueState[TaxiRide] = getRuntimeContext.getState(
      new ValueStateDescriptor[TaxiRide]("started-ride", TypeInformation.of(new TypeHint[TaxiRide]() {})))
    lazy val rideEndedState: ValueState[TaxiRide] = getRuntimeContext.getState(
      new ValueStateDescriptor[TaxiRide]("ended-ride", TypeInformation.of(new TypeHint[TaxiRide]() {})))

    override def processElement(ride: TaxiRide, context: ProcessFunction[TaxiRide, TaxiRide]#Context, out: Collector[TaxiRide]): Unit = {
      val timerService = context.timerService

      if (ride.isStart) {
        rideStartedState.update(ride)
        // set a timer for 120 event-time minutes after the ride started
        timerService.registerEventTimeTimer(ride.getEventTime + 120 * 60 * 1000)
      }
      else {
        if (rideStartedState.value != null) {
          rideEndedState.update(ride)
        }
        else {
          // There either was no matching START event, or
          // this is a long ride and the START has already been reported and cleared.
          // In either case, we should not create any state, since it will never get cleared.

          // out.collect(ride)
        }
      }
    }

    override def onTimer(timestamp: Long, ctx: ProcessFunction[TaxiRide, TaxiRide]#OnTimerContext, out: Collector[TaxiRide]): Unit = {
      val rideStarted = rideStartedState.value
      val rideEnded = rideEndedState.value

      if (rideStarted != null && rideEnded == null) {
        out.collect(rideStarted)
      }
      rideStartedState.clear()
      rideEndedState.clear()
    }
  }

}
