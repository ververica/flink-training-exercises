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

package com.dataArtisans.flinkTraining.exercises.dataStreamJava.accidentDelays;

import com.dataArtisans.flinkTraining.exercises.dataStreamJava.dataTypes.Accident;
import com.dataArtisans.flinkTraining.exercises.dataStreamJava.rideCleansing.RideCleansing;
import com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils.AccidentGenerator;
import com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils.GeoUtils;
import com.dataArtisans.flinkTraining.exercises.dataStreamJava.dataTypes.TaxiRide;
import com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils.TaxiRideGenerator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AccidentDelays {

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		String input = params.getRequired("input");
		float servingSpeedFactor = params.getFloat("speed", 1.0f);

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// create taxi ride stream
		DataStream<TaxiRide> rides = env.addSource(new TaxiRideGenerator(input, servingSpeedFactor))
				.filter(new RideCleansing.NYCFilter());

		// create accidents stream
		DataStream<Tuple2<Integer,Accident>> accidents = env
				.addSource(new AccidentGenerator(servingSpeedFactor))
				.map(new AccidentCellMapper())
				// group by accident cell id
				.partitionByHash(0);

		DataStream<Tuple2<Integer, TaxiRide>> rideAccidents = rides
				.flatMap(new RouteCellMapper())
				// group by route cell id
				.partitionByHash(0)
				.connect(accidents)
				.flatMap(new AccidentsPerRideCounter());

		rideAccidents
				.print();

		// run the transformation pipeline
		env.execute("Accident Delayed Rides");
	}

	public static class AccidentCellMapper implements MapFunction<Accident, Tuple2<Integer, Accident>> {

		Tuple2<Integer, Accident> outT = new Tuple2<Integer, Accident>();

		@Override
		public Tuple2<Integer, Accident> map(Accident accident) throws Exception {
			outT.f0 = GeoUtils.mapToGridCell(accident.lon, accident.lat);
			outT.f1 = accident;
			return outT;
		}
	}

	public static class RouteCellMapper implements FlatMapFunction<TaxiRide, Tuple2<Integer, TaxiRide>> {

		Tuple2<Integer, TaxiRide> outT = new Tuple2<Integer, TaxiRide>();

		@Override
		public void flatMap(TaxiRide taxiRide, Collector<Tuple2<Integer, TaxiRide>> out) throws Exception {

			List<Integer> routeCellIds = GeoUtils.mapToGridCellsOnWay(
					taxiRide.startLon, taxiRide.startLat,
					taxiRide.endLon, taxiRide.endLat);

			outT.f1 = taxiRide;
			outT.f0 = routeCellIds.get(0);

			out.collect(outT);
			for(Integer cellId : routeCellIds) {
				outT.f0 = cellId;
				out.collect(outT);
			}
		}
	}

	public static class AccidentsPerRideCounter implements CoFlatMapFunction<
			Tuple2<Integer, TaxiRide>,
			Tuple2<Integer, Accident>,
			Tuple2<Integer, TaxiRide>> {

		private HashMap<Integer, Set<Long>> accidentsByCell = new HashMap<Integer, Set<Long>>();
		private HashMap<Integer, Set<TaxiRide>> ridesByCell = new HashMap<Integer, Set<TaxiRide>>();
		private Tuple2<Integer, TaxiRide> outT = new Tuple2<Integer, TaxiRide>();

		@Override
		public void flatMap1(Tuple2<Integer, TaxiRide> ride, Collector<Tuple2<Integer, TaxiRide>> out) throws Exception {

			// new ride event
			int cell = ride.f0;

			if (ride.f1.isStart) {

				// check accidents on cell
				Set<Long> accidents = accidentsByCell.get(cell);
				boolean accidentOnCell = (accidents != null && accidents.size() > 0);

				if (accidentOnCell) {
					// emit ride directly
					out.collect(ride);
				} else {
					// remember ride
					Set<TaxiRide> ridesOnCell = this.ridesByCell.get(cell);
					if (ridesOnCell == null) {
						ridesOnCell = new HashSet<TaxiRide>();
						this.ridesByCell.put(cell, ridesOnCell);
					}
					ridesOnCell.add(ride.f1);
				}
			} else {

				// remove ride
				Set<TaxiRide> ridesOnCell = this.ridesByCell.get(cell);
				if (ridesOnCell != null) {
					ridesOnCell.remove(ride.f1);
				}
			}
		}

		@Override
		public void flatMap2(Tuple2<Integer, Accident> accident, Collector<Tuple2<Integer, TaxiRide>> out) throws Exception {

			// new accident event
			int cell = accident.f0;

			if (!accident.f1.isCleared) {

				// emit all rides on cell
				Set<TaxiRide> ridesOnCell = this.ridesByCell.get(cell);
				if (ridesOnCell != null) {
					outT.f0 = cell;
					for (TaxiRide ride : ridesOnCell) {
						outT.f1 = ride;
						out.collect(outT);
					}
					// clear all rides
					ridesOnCell.clear();
				}

				// add accident
				Set<Long> accidentsOnCell = accidentsByCell.get(cell);
				if (accidentsOnCell == null) {
					accidentsOnCell = new HashSet<Long>();
					this.accidentsByCell.put(cell, accidentsOnCell);
				}
				accidentsOnCell.add(accident.f1.accidentId);

			} else {

				// remove accident
				Set<Long> accidentIds = accidentsByCell.get(cell);
				if (accidentIds != null) {
					accidentIds.remove(accident.f1.accidentId);
				}
			}
		}
	}

}
