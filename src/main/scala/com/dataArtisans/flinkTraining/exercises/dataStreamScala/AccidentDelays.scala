/*
 * Copyright 2015 data Artisans GmbH
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

package com.dataArtisans.flinkTraining.exercises.dataStreamScala

import com.dataArtisans.flinkTraining.exercises.dataStreamJava.dataTypes.{TaxiRide, Accident}
import com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils.{GeoUtils, AccidentGenerator, TaxiRideGenerator}
import org.apache.flink.api.common.functions.{MapFunction, FlatMapFunction}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction

import scala.collection.JavaConverters._

import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable

object AccidentDelays {

  def main(args: Array[String]) {

    val params = ParameterTool.fromArgs(args)
    val input = params.getRequired("input")
    val servingSpeedFactor = params.getFloat("speed", 1.0f)

    // set up streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // create taxi ride stream
    val rides = env
      .addSource(new TaxiRideGenerator(input, servingSpeedFactor))
      .filter(r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat))

    // create accidents stream
    val accidents = env
      .addSource(new AccidentGenerator(servingSpeedFactor))
      .map(new AccidentCellMapper)
      .partitionByHash(0) // group by accident cell id

    val rideAccidents = rides
      .flatMap(new RouteCellMapper)
      .partitionByHash(0) // group by route cell id
      .connect(accidents)
      .flatMap(new AccidentsPerRideCounter)

    rideAccidents.print()

    env.execute("Accident Delayed Rides")
  }

  class AccidentCellMapper extends MapFunction[Accident, (Int, Accident)] {

    def map(accident: Accident): (Int, Accident) = {
      val gridCell = GeoUtils.mapToGridCell(accident.lon, accident.lat)
      (gridCell, accident)
    }
  }

  class RouteCellMapper extends FlatMapFunction[TaxiRide, (Int, TaxiRide)] {

    def flatMap(taxiRide: TaxiRide, out: Collector[(Int, TaxiRide)]) {

      val routeCellIds = GeoUtils.mapToGridCellsOnWay(
        taxiRide.startLon,
        taxiRide.startLat,
        taxiRide.endLon,
        taxiRide.endLat).asScala

      routeCellIds foreach { id => out.collect((id, taxiRide))}
    }
  }

  class AccidentsPerRideCounter extends CoFlatMapFunction[(Int, TaxiRide), (Int, Accident), (Int, TaxiRide)] {

    private val accidentsByCell = mutable.HashMap.empty[Int, mutable.Set[Long]]
    private val ridesByCell = mutable.HashMap.empty[Int, mutable.Set[TaxiRide]]


    def flatMap1(ride: (Int, TaxiRide), out: Collector[(Int, TaxiRide)]) {
      // new ride event
      val cell = ride._1

      if (ride._2.isStart) {
        // check accidents on cell
        val accidents = accidentsByCell.getOrElseUpdate(cell, mutable.Set.empty[Long])
        if (accidents.nonEmpty) {
          // emit ride directly
          out.collect(ride)
        } else {
          // remember ride
          val set = ridesByCell.getOrElseUpdate(cell, mutable.Set.empty[TaxiRide])
          set.add(ride._2)
        }
      } else {
        ridesByCell.get(cell) match {
          case Some(set) => set.remove(ride._2) // remove ride
          case _ => // we don't have the ride stored
        }
      }
    }

    def flatMap2(accident: (Int, Accident), out: Collector[(Int, TaxiRide)]) {
      // new accident event
      val cell = accident._1

      if (!accident._2.isCleared) {
        // emit all rides on cell
        ridesByCell.remove(cell) match {
          case Some(set) => set foreach { ride => out.collect((cell, ride)) }
          case _ => // nothing to do
        }

        // add accident
        val set = accidentsByCell.getOrElseUpdate(cell, mutable.Set.empty[Long])
        set.add(accident._2.accidentId)
      } else {
        // remove accident

        accidentsByCell.get(cell) match {
          case Some(set) => set.remove(accident._2.accidentId)
          case _ => // Nothing to do
        }
      }
    }
  }

}

