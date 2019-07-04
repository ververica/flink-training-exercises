/*
 * Copyright 2017 data Artisans GmbH, 2019 Ververica GmbH
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

package com.ververica.flinktraining.exercises.datastream_scala.sources

import org.apache.flink.api.scala._
import com.ververica.flinktraining.exercises.datastream_java.datatypes.{Customer, Trade}
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark

object FinSources {
  /**
    * This source generates a stream of customer events
    *
    * @param env
    * @return
    */

  def customerSource(env: StreamExecutionEnvironment): DataStream[Customer] = {
    // This is a bit of a hack to use Thread.sleep() for sequencing but it works for our test purposes
     env.addSource((sc: SourceContext[Customer]) => {
       sc.collectWithTimestamp(new Customer(0L, 0L, "Customer data @ 0"), 0)
       sc.emitWatermark(new Watermark(0))
       Thread.sleep(2000)
       sc.collectWithTimestamp(new Customer(500L, 0L, "Customer data @ 500"), 500)
       sc.emitWatermark(new Watermark(500))
       Thread.sleep(1000)
       sc.collectWithTimestamp(new Customer(1500L, 0L, "Customer data @ 1500"), 1500)
       sc.emitWatermark(new Watermark(1500))
       Thread.sleep(6000)
       sc.collectWithTimestamp(new Customer(1600L, 0L, "Customer data @ 1600"), 1600)
       sc.emitWatermark(new Watermark(1600))
       Thread.sleep(1000)
       sc.collectWithTimestamp(new Customer(2100L, 0L, "Customer data @ 2100"), 2100)
       sc.emitWatermark(new Watermark(2100))

       while (true) {
         Thread.sleep(1000)
       }
     })
  }

  /**
    * This source generates the stream of trades
    *
    *
    * @param env
    * @return
    */

  def tradeSource(env: StreamExecutionEnvironment): DataStream[Trade] = {
    env.addSource((sc: SourceContext[Trade]) => {
      Thread.sleep(1000)
      sc.collectWithTimestamp(new Trade(1000L, 0L, "trade-1"), 1000)
      sc.emitWatermark(new Watermark(1000))
      Thread.sleep(3000)
      sc.collectWithTimestamp(new Trade(1200L, 0L, "trade-2"), 1200)
      sc.emitWatermark(new Watermark(1200))
      Thread.sleep(1000)
      sc.collectWithTimestamp(new Trade(1500L, 0L, "trade-3"), 1500)
      sc.emitWatermark(new Watermark(1500))
      Thread.sleep(1000)
      sc.collectWithTimestamp(new Trade(1700L, 0L, "trade-4"), 1700)
      sc.emitWatermark(new Watermark(1700))
      Thread.sleep(1000)
      sc.collectWithTimestamp(new Trade(1800L, 0L, "trade-5"), 1800)
      sc.emitWatermark(new Watermark(1800))
      Thread.sleep(1000)
      sc.collectWithTimestamp(new Trade(2000L, 0L, "trade-6"), 2000)
      sc.emitWatermark(new Watermark(2000))

      while (true) {
        Thread.sleep(1000)
      }
    })
  }
}