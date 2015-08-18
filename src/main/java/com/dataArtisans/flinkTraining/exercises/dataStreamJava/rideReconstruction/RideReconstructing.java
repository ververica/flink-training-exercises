/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataArtisans.flinkTraining.exercises.dataStreamJava.rideReconstruction;

import com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils.GeoUtils;
import com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils.TaxiRide;
import com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils.TaxiRideGenerator;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.WindowMapFunction;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.util.Collector;

import java.util.List;

public class RideReconstructing {

  // TODO remove this
  public static String inputPath = "/home/fixcqa3/dev/projects/flink-training/dataSets/trips_start_end_sorted_1.csv";
  public static float servingSpeedFactor = 500.0f;

  public static void main(String[] args) throws Exception {

    /* set up streaming execution environment */
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    /* create a data source */
    //DataStream<TaxiRide> rides =
    //      env.addSource(new KafkaSource<TaxiRide>("localhost:9092", "flink-streaming", new TaxiRideSchema()));
    DataStream<TaxiRide> rides = env.addSource(new TaxiRideGenerator(inputPath, servingSpeedFactor)); // TODO remove this

    /* reconstruct taxi ride from incoming events */
    DataStream<ReconstructedTaxiRide> reconstructedRides = rides.groupBy("rideId")
      .window(Count.of(2)).every(Count.of(2))
      .mapWindow(new RideReconstructor())
      .flatten()
      .filter(new DistanceFilter());

    /* emit the result on stdout */
    reconstructedRides.print();

    /* run the transformation pipeline */
    env.execute("Basic Exercise 1");
  }

  /** WindowMapFunction to reconstruct original data.  */
  static class RideReconstructor implements WindowMapFunction<TaxiRide, ReconstructedTaxiRide> {
    @Override
    public void mapWindow(Iterable<TaxiRide> values, Collector<ReconstructedTaxiRide> out)
    throws Exception {
      /* list contains start and stop TaxiRide */
      List<TaxiRide> taxiRideList = IteratorUtils.toList(values.iterator());

      /* get the start and end taxi ride */
      TaxiRide start = taxiRideList.get(0);
      TaxiRide end = taxiRideList.get(1);

      /* compute trip duration and distance */
      long rideDurationMillis = Math.abs(end.time.getMillis() - start.time.getMillis());
      float rideDistance = GeoUtils.computeEuclideanDistance(start, end);

      out.collect(new ReconstructedTaxiRide(start.rideId, start.startLon, start.startLat, end.endLon, end.endLat,
              start.passengerCnt, end.travelDistance, rideDistance, rideDurationMillis));
    }
  }

  /** POJO for reconstructed taxi ride. */
  static class ReconstructedTaxiRide {
    public long rideId;
    public float startLon;
    public float startLat;
    public float endLon;
    public float endLat;
    public short passengerCnt;
    public float rideDistanceOrigin;
    public float rideDistanceComputed;
    public long rideDurationMillis;

    public ReconstructedTaxiRide(long rideId, float startLon, float startLat, float endLon, float endLat,
                                 short passengerCnt, float rideDistanceOrigin, float rideDistanceComputed,
                                 long rideDurationMillis) {
      this.rideId = rideId;
      this.startLon = startLon;
      this.startLat = startLat;
      this.endLon = endLon;
      this.endLat = endLat;
      this.passengerCnt = passengerCnt;
      this.rideDistanceOrigin = rideDistanceOrigin;
      this.rideDistanceComputed = rideDistanceComputed;
      this.rideDurationMillis = rideDurationMillis;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(this.rideId).append(",");
      sb.append(this.startLon).append(",");
      sb.append(this.startLat).append(",");
      sb.append(this.endLon).append(",");
      sb.append(this.endLat).append(",");
      sb.append(this.passengerCnt).append(",");
      sb.append(this.rideDistanceOrigin).append(",");
      sb.append(this.rideDistanceComputed).append(",");
      sb.append(this.rideDurationMillis);

      return sb.toString();
    }

  }

  /** FilterFunction to filter out taxi rides with invalid distance information. */
  private static class DistanceFilter implements FilterFunction<ReconstructedTaxiRide> {
    @Override
    public boolean filter(ReconstructedTaxiRide ride) throws Exception {
      /* keep rides with valid origin distance information */
      return ride.rideDistanceOrigin > ride.rideDistanceComputed;
    }

  }

}
