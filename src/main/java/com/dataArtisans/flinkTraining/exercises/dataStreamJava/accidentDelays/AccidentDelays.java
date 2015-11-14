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

/**
 * Java reference implementation for the "Accident Delays" exercise of the Flink training (http://dataartisans.github.io/flink-training).
 * The task of the exercise is to connect a data stream of taxi rides and a stream of accident reports to identify taxi rides that
 * might have been delayed due to accidents.
 *
 * Parameters:
 *   --input path-to-input-directory
 *   --speed serving-speed-of-generator
 *
 */
public class AccidentDelays {

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		String input = params.getRequired("input");
		float servingSpeedFactor = params.getFloat("speed", 1.0f);

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// create taxi ride stream
		DataStream<TaxiRide> rides = env.addSource(new TaxiRideGenerator(input, servingSpeedFactor))
				// filter rides which do not start and end in NYC
				.filter(new RideCleansing.NYCFilter());

		// create accidents stream
		DataStream<Tuple2<Integer,Accident>> accidents = env
				.addSource(new AccidentGenerator(servingSpeedFactor))
				// map accident to grid cell
				.map(new AccidentCellMapper())
				// group by accident cell id
				.keyBy(0);

		DataStream<Tuple2<Integer, TaxiRide>> rideAccidents = rides
				// map taxi ride to all grid cells on its way
				.flatMap(new RouteCellMapper())
				// group by route cell id
				.keyBy(0)
				// connect streams and match rides and accidents on the same grid cell
				.connect(accidents)
				.flatMap(new AccidentsPerRideCounter());

		rideAccidents.print();

		// run the transformation pipeline
		env.execute("Accident Delayed Rides");
	}

	/**
	 * Maps an Accident to the grid cell id of its location.
	 */
	public static class AccidentCellMapper implements MapFunction<Accident, Tuple2<Integer, Accident>> {

		Tuple2<Integer, Accident> outT = new Tuple2<Integer, Accident>();

		@Override
		public Tuple2<Integer, Accident> map(Accident accident) throws Exception {
			outT.f0 = GeoUtils.mapToGridCell(accident.lon, accident.lat);
			outT.f1 = accident;
			return outT;
		}
	}

	/**
	 * Maps a TaxiRide to all grid cells between its start and its end location.
	 * For each grid cell on the way, a record is emitted.
	 */
	public static class RouteCellMapper implements FlatMapFunction<TaxiRide, Tuple2<Integer, TaxiRide>> {

		Tuple2<Integer, TaxiRide> outT = new Tuple2<Integer, TaxiRide>();

		@Override
		public void flatMap(TaxiRide taxiRide, Collector<Tuple2<Integer, TaxiRide>> out) throws Exception {

			// get all grid cells on the way from start to end of the ride
			List<Integer> routeCellIds = GeoUtils.mapToGridCellsOnWay(
					taxiRide.startLon, taxiRide.startLat,
					taxiRide.endLon, taxiRide.endLat);

			outT.f1 = taxiRide;
			
			for(Integer cellId : routeCellIds) {
				// emit a record for each grid cell
				outT.f0 = cellId;
				out.collect(outT);
			}
		}
	}

	/**
	 * Matches taxi rides which pass accidents on the same grid cell.
	 * Accidents are kept until a clearance event is received.
	 */
	public static class AccidentsPerRideCounter implements CoFlatMapFunction<
			Tuple2<Integer, TaxiRide>,
			Tuple2<Integer, Accident>,
			Tuple2<Integer, TaxiRide>> {

		// holds accidents indexed by cell id
		private HashMap<Integer, Set<Long>> accidentsByCell = new HashMap<Integer, Set<Long>>();
		// holds taxi rides indexed by cell id
		private HashMap<Integer, Set<TaxiRide>> ridesByCell = new HashMap<Integer, Set<TaxiRide>>();
		// the output record
		private Tuple2<Integer, TaxiRide> outT = new Tuple2<Integer, TaxiRide>();

		@Override
		public void flatMap1(Tuple2<Integer, TaxiRide> ride, Collector<Tuple2<Integer, TaxiRide>> out) throws Exception {

			// new ride event
			int cell = ride.f0;

			if (ride.f1.isStart) {
				// ride event is a start event

				// check if an accident happened on the cell
				Set<Long> accidents = accidentsByCell.get(cell);
				boolean accidentOnCell = (accidents != null && accidents.size() > 0);

				if (accidentOnCell) {
					// emit ride directly and do not remember it
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
				// ride event is an end event

				// forget ride
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
				// accident event is an emergence event

				// check if taxi rides pass this cell
				Set<TaxiRide> ridesOnCell = this.ridesByCell.get(cell);
				if (ridesOnCell != null) {
					// emit all rides on cell
					outT.f0 = cell;
					for (TaxiRide ride : ridesOnCell) {
						outT.f1 = ride;
						out.collect(outT);
					}
					// forget all rides on cell
					ridesOnCell.clear();
				}

				// remember accident
				Set<Long> accidentsOnCell = accidentsByCell.get(cell);
				if (accidentsOnCell == null) {
					accidentsOnCell = new HashSet<Long>();
					this.accidentsByCell.put(cell, accidentsOnCell);
				}
				accidentsOnCell.add(accident.f1.accidentId);

			} else {
				// accident event is a clearance event

				// forget accident
				Set<Long> accidentIds = accidentsByCell.get(cell);
				if (accidentIds != null) {
					accidentIds.remove(accident.f1.accidentId);
				}
			}
		}
	}

}
