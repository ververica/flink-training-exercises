package com.dataartisans.flinktraining.exercises.datastream_scala.broadcast

import java.util.Map.Entry

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource
import com.dataartisans.flinktraining.exercises.datastream_java.utils.{ExerciseBase, MissingSolutionException}
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase.{printOrTest, rideSourceOrTest}
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.util.Random

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

    val rides = env.addSource(rideSourceOrTest(
      new TaxiRideSource(ridesFile, maxEventDelay, servingSpeedFactor)))

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
      .keyBy(_._1)
      .process(new ClosestTaxi)

    printOrTest(nearest)

    env.execute("Nearest Available Taxi")
  }

  // Only pass thru values that are new minima -- remove duplicates.
  class ClosestTaxi extends KeyedProcessFunction[Long, (Long, Long, Float), (Long, Long, Float)] {
    // store (taxiId, distance), keyed by queryId
    lazy val closetState: ValueState[(Long, Float)] = getRuntimeContext.getState(
      new ValueStateDescriptor[(Long, Float)]("report", createTypeInformation[(Long, Float)]))

    // in and out tuples: (queryId, taxiId, distance)
    override def processElement(report: (Long, Long, Float),
        context: KeyedProcessFunction[Long, (Long, Long, Float), (Long, Long, Float)]#Context,
        out: Collector[(Long, Long, Float)]): Unit =
      if (closetState.value == null || report._3 < closetState.value._2) {
        closetState.update((report._2, report._3))
        out.collect(report)
      }
  }

  // Note that in order to have consistent results after a restore from a checkpoint, the
  // behavior of this method must be deterministic, and NOT depend on characterisitcs of an
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
