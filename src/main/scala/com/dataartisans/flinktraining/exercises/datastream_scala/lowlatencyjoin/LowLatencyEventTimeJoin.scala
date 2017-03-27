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

package com.dataartisans.flinktraining.exercises.datastream_scala.lowlatencyjoin

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction.{Context, OnTimerContext}
import org.apache.flink.streaming.api.functions.co.RichCoProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector

import scala.collection.mutable


object LowLatencyEventTimeJoin {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // Simulated trade stream
    val tradeStream = Sources.tradeSource(env)

    // simulated customer stream
    val customerStream = Sources.customerSource(env)

    val joinedStream = tradeStream
      .keyBy(_.customerId)
      .connect(customerStream.keyBy(_.customerId))
      .process(new EventTimeJoinFunction)

    joinedStream.print()

    env.execute
  }
}

/**
  * This is a homegrown join function using the new Flink 1.2 ProcessFunction.
  * Basically, what we do is the following:
  *
  * 1) When we receive a trade we join it against the customer data right away, however
  * to keep this 100% deterministic we join against the latest customer data that
  * has a timestamp LESS THAN the trade timestamp -- not simply the latest available data.
  * In other words we are joining against the customer data that we knew at the time of the trade.
  *
  * 2) We also set a trigger to evaluate the trade again once the watermark has passed the trade
  * time.  Basically, what we're doing here is using event time to ensure that we have
  * "complete" data and then joining again at that time. This will give us a deterministic
  * result even in the face of undordered data, etc
  *
  * This approach has the benefit that you don't introduce any latency into the trade stream
  * because you always join right away.  You then emit a BETTER result if you receive better
  * information.  We use event time in order to know how long we must wait for this potential
  * better information.
  *
  * We also use event time to know when it's safe to expire state.
  */
class EventTimeJoinFunction extends RichCoProcessFunction[Trade, Customer, EnrichedTrade] {

  private var tradeBufferState: ValueState[mutable.PriorityQueue[EnrichedTrade]] = null
  private var customerBufferState: ValueState[mutable.PriorityQueue[Customer]] = null

  override def open(parameters: Configuration): Unit = {
    implicit val tradeOrdering = Ordering.by((t: EnrichedTrade) => t.trade.timestamp)
    implicit val customerOrdering = Ordering.by((c: Customer) => c.timestamp)
    tradeBufferState = getRuntimeContext.getState(new ValueStateDescriptor[mutable.PriorityQueue[EnrichedTrade]]("tradeBuffer", createTypeInformation[mutable.PriorityQueue[EnrichedTrade]], new mutable.PriorityQueue[EnrichedTrade]))
    customerBufferState = getRuntimeContext.getState(new ValueStateDescriptor[mutable.PriorityQueue[Customer]]("customerBuffer", createTypeInformation[mutable.PriorityQueue[Customer]], new mutable.PriorityQueue[Customer]))
  }

  override def processElement1(trade: Trade, context: Context, collector: Collector[EnrichedTrade]): Unit = {
    println(s"Received: $trade")

    val timerService = context.timerService()
    val joinedData = join(trade)
    collector.collect(joinedData)
    if (context.timestamp() > timerService.currentWatermark()) {
      val tradeBuffer = tradeBufferState.value()
      tradeBuffer.enqueue(joinedData)
      tradeBufferState.update(tradeBuffer)
      timerService.registerEventTimeTimer(trade.timestamp)
    } else {
      // TODO : Handle late data -- detect and join against what, latest?  Drop it?
    }
  }

  override def processElement2(customer: Customer, context: Context, collector: Collector[EnrichedTrade]): Unit = {
    println(s"Received $customer")

    val customerBuffer = customerBufferState.value()
    customerBuffer.enqueue(customer)
    customerBufferState.update(customerBuffer)
  }

  override def onTimer(l: Long, context: OnTimerContext, collector: Collector[EnrichedTrade]): Unit = {
    // look for trades that can now be completed, do the join, and remove from the tradebuffer
    val tradeBuffer = tradeBufferState.value()
    val watermark = context.timerService().currentWatermark()
    while (tradeBuffer.headOption.map(_.trade.timestamp).getOrElse(Long.MaxValue) <= watermark) {
      val enrichedTrade = tradeBuffer.dequeue()
      val joinedData = join(enrichedTrade.trade)

      // Only emit again if we have better data
      if (!joinedData.equals(enrichedTrade)) {
        collector.collect(joinedData)
      }
    }
    tradeBufferState.update(tradeBuffer)

    // Cleanup all the customer data that is eligible
    cleanupEligibleCustomerData(watermark)
  }

  private def cleanupEligibleCustomerData(watermark: Long): Unit = {
    // Keep all the customer data that is newer than the watermark PLUS
    // the most recent element that is older than the watermark.
    val customerBuffer = customerBufferState.value()
    val (above, below) = customerBuffer.partition(_.timestamp >= watermark)
    below.headOption.foreach(above.enqueue(_))
    customerBufferState.update(above)
  }

  private def join(trade: Trade): EnrichedTrade = {
    // get the customer info that was in effect at the time of this trade
    // doing this rather than jumping straight to the latest known info makes
    // this 100% deterministic.  If that's not a strict requirement we can simplify
    // this by joining against the latest available data right now.
    val customerBuffer = customerBufferState.value()
    val customerInfo = customerBuffer
      .filter(_.timestamp <= trade.timestamp)
      .headOption
      .map(_.customerInfo)
      .getOrElse("No customer info available")

    EnrichedTrade(trade, customerInfo)
  }
}

object Sources {
  /**
    * This source generates a stream of customer events (CDC info)
    *
    * @param env
    * @return
    */
  def customerSource(env: StreamExecutionEnvironment): DataStream[Customer] = {
    // TODO: This is a bit of a hack to use Thread.sleep() for sequencing but it works for our test purposes
    env.addSource((sc: SourceContext[Customer]) => {
      sc.collectWithTimestamp(Customer(0, 0, "Customer data @ 0"), 0)
      sc.emitWatermark(new Watermark(0))
      Thread.sleep(2000)
      sc.collectWithTimestamp(Customer(500, 0, "Customer data @ 500"), 500)
      sc.emitWatermark(new Watermark(500))
      Thread.sleep(1000)
      sc.collectWithTimestamp(Customer(1500, 0, "Customer data @ 1500"), 1500)
      sc.emitWatermark(new Watermark(1500))
      Thread.sleep(6000)
      sc.collectWithTimestamp(Customer(1600, 0, "Customer data @ 1600"), 1600)
      sc.emitWatermark(new Watermark(1600))
      Thread.sleep(1000)
      sc.collectWithTimestamp(Customer(2100, 0, "Customer data @ 2100"), 2100)
      sc.emitWatermark(new Watermark(2100))

      while (true) {
        Thread.sleep(1000);
      }
    })
  }

  /**
    * This source generates the stream of trades
    *
    * @param env
    * @return
    */
  def tradeSource(env: StreamExecutionEnvironment): DataStream[Trade] = {
    env.addSource((sc: SourceContext[Trade]) => {
      Thread.sleep(1000)
      sc.collectWithTimestamp(Trade(1000, 0, "trade-1"), 1000)
      sc.emitWatermark(new Watermark(1000))
      Thread.sleep(3000)
      sc.collectWithTimestamp(Trade(1200, 0, "trade-2"), 1200)
      sc.emitWatermark(new Watermark(1200))
      Thread.sleep(1000)
      sc.collectWithTimestamp(Trade(1500, 0, "trade-3"), 1500)
      sc.emitWatermark(new Watermark(1500))
      Thread.sleep(1000)
      sc.collectWithTimestamp(Trade(1700, 0, "trade-4"), 1700)
      sc.emitWatermark(new Watermark(1700))
      Thread.sleep(1000)
      sc.collectWithTimestamp(Trade(1800, 0, "trade-5"), 1800)
      sc.emitWatermark(new Watermark(1800))
      Thread.sleep(1000)
      sc.collectWithTimestamp(Trade(2000, 0, "trade-6"), 2000)
      sc.emitWatermark(new Watermark(2000))

      while (true) {
        Thread.sleep(1000)
      }
    })
  }
}
