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

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.{Customer, EnrichedTrade, Trade}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoProcessFunction

import scala.collection.mutable

abstract class EventTimeJoinHelper extends RichCoProcessFunction[Trade, Customer, EnrichedTrade] {

  protected var tradeBufferState: ValueState[mutable.PriorityQueue[EnrichedTrade]] = null
  protected var customerBufferState: ValueState[mutable.PriorityQueue[Customer]] = null

  override def open(parameters: Configuration): Unit = {
    tradeBufferState = getRuntimeContext.getState(new ValueStateDescriptor[mutable.PriorityQueue[EnrichedTrade]]("tradeBuffer", createTypeInformation[mutable.PriorityQueue[EnrichedTrade]]))
    customerBufferState = getRuntimeContext.getState(new ValueStateDescriptor[mutable.PriorityQueue[Customer]]("customerBuffer", createTypeInformation[mutable.PriorityQueue[Customer]]))
  }

  protected def timestampOfFirstTrade(): Long = {
    val tradeBuffer = tradeBufferState.value()
    tradeBuffer.headOption.map(_.trade.timestamp.toLong).getOrElse(Long.MaxValue)
  }

  protected def cleanupEligibleCustomerData(watermark: Long): Unit = {
    // Keep all the customer data that is newer than the watermark PLUS
    // the most recent element that is older than the watermark.
    val customerBuffer = customerBufferState.value()
    val (above, below) = customerBuffer.partition(_.timestamp >= watermark)
    below.headOption.foreach(above.enqueue(_))
    customerBufferState.update(above)
  }

  protected def dequeueEnrichedTrade(): EnrichedTrade = {
    val tradeBuffer = tradeBufferState.value()
    val enrichedTrade = tradeBuffer.dequeue()
    tradeBufferState.update(tradeBuffer)
    enrichedTrade
  }

  protected def enqueueEnrichedTrade(joinedData: EnrichedTrade) = {
    implicit val tradeOrdering = Ordering.by((t: EnrichedTrade) => t.trade.timestamp)
    var tradeBuffer = tradeBufferState.value()
    if (tradeBuffer == null) {
      tradeBuffer = new mutable.PriorityQueue[EnrichedTrade]
    }
    tradeBuffer.enqueue(joinedData)
    tradeBufferState.update(tradeBuffer)
  }

  protected def enqueueCustomer(customer: Customer) = {
    implicit val customerOrdering = Ordering.by((c: Customer) => c.timestamp)
    var customerBuffer = customerBufferState.value()
    if (customerBuffer == null) {
      customerBuffer = new mutable.PriorityQueue[Customer]
    }
    customerBuffer.enqueue(customer)
    customerBufferState.update(customerBuffer)
  }

  protected def customerIterator(): Iterator[Customer] = {
    customerBufferState.value().iterator
  }
}
