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

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.{Customer, EnrichedTrade, Trade}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.CoProcessFunction.{Context, OnTimerContext}
import org.apache.flink.util.Collector

class EventTimeJoinFunction extends EventTimeJoinHelper {

  override def processElement1(trade: Trade, context: Context, collector: Collector[EnrichedTrade]): Unit = {
    println(s"Scala Received: $trade")

    val timerService = context.timerService()
    val joinedData = join(trade)
    collector.collect(joinedData)
    if (context.timestamp() > timerService.currentWatermark()) {
      enqueueEnrichedTrade(joinedData)
      timerService.registerEventTimeTimer(trade.timestamp)
    } else {
      // Handle late data -- detect and join against what, latest?  Drop it?
    }
  }

  override def processElement2(customer: Customer, context: Context, collector: Collector[EnrichedTrade]): Unit = {
    println(s"Scala Received $customer")
    enqueueCustomer(customer)
  }

  override def onTimer(l: Long, context: OnTimerContext, collector: Collector[EnrichedTrade]): Unit = {
    // look for trades that can now be completed, do the join, and remove from the tradebuffer
    val watermark: Long = context.timerService().currentWatermark()
    while (timestampOfFirstTrade() <= watermark) {
      dequeueAndPerhapsEmit(collector)
    }

    // Cleanup all the customer data that is eligible
    cleanupEligibleCustomerData(watermark)
  }

  private def join(trade: Trade): EnrichedTrade = {
    // get the customer info that was in effect at the time of this trade
    // doing this rather than jumping straight to the latest known info makes
    // this 100% deterministic.  If that's not a strict requirement we can simplify
    // this by joining against the latest available data right now.
    new EnrichedTrade(trade, getCustomerInfo(trade))
  }

  private def getCustomerInfo(trade: Trade): String = {
    customerBufferState.value()
      .filter(_.timestamp <= trade.timestamp)
      .headOption
      .map(_.customerInfo)
      .getOrElse("No customer info available")
  }

  protected def dequeueAndPerhapsEmit(collector: Collector[EnrichedTrade]): Unit = {
    val enrichedTrade = dequeueEnrichedTrade()

    val joinedData = join(enrichedTrade.trade)
    // Only emit again if we have better data
    if (!joinedData.equals(enrichedTrade)) {
      collector.collect(joinedData)
    }
  }
}
