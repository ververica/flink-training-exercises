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

package com.dataArtisans.flinkTraining.exercises.dataStreamJava.girdMatching;

import com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils.GeoUtils;
import com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils.TaxiRide;
import com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils.TaxiRideGenerator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.WindowMapFunction;
import org.apache.flink.streaming.api.windowing.helper.Time;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class GridMatching {

  public static String inputPath = "/tmp/trips_start_end_sorted_1.csv";
  public static float servingSpeedFactor = 50.0f;
  public static float popularityIndex = 10;

  public static void main(String[] args) throws Exception {
    /* parse arguments if given */
    if(!parseArguments(args)) {
      return;
    }

    /* adjust window size and eviction interval to fast-forward factor */
    int windowSize = (int)((15 * 60 * 1000) / servingSpeedFactor);
    int evictionInterval = (int)((5 * 60 * 1000) / servingSpeedFactor);

    /* set up streaming execution environment */
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    /* start the data generator */
    DataStream<TaxiRide> rides = env.addSource(new TaxiRideGenerator(inputPath, servingSpeedFactor));

    /* find n most popular spots */
    DataStream<Tuple2<Integer, Integer>> popularSpots = rides.filter(new GridFilter())
      .map(new GridMatcher())
      .groupBy(new CustomKeySelector())
      .window(Time.of(windowSize, TimeUnit.MILLISECONDS)).every(Time.of(evictionInterval, TimeUnit.MILLISECONDS))
      .mapWindow(new PopularityCounter())
      .flatten()
      .filter(new PopularityFilter());

    /* print result on stdout */
    popularSpots.print();

    /* execute the transformation pipeline */
    env.execute("Advanced Exercise 1");
  }

  /** FilterFunction to filter out TaxiRides not starting / ending within NYC boundaries.  */
  static class GridFilter implements FilterFunction<TaxiRide> {
    @Override
    public boolean filter(TaxiRide taxiRide) throws Exception {
      /* filter out rides which do not start OR stop in NYC */
      return GeoUtils.isInNYC(taxiRide);
    }

  }

  /** MapFunction to map start / end location of a TaxiRide to a cell Id */
  static class GridMatcher implements MapFunction<TaxiRide, Tuple2<TaxiRide, Integer>> {
    @Override
    public Tuple2<TaxiRide, Integer> map(TaxiRide taxiRide) throws Exception {
      if(taxiRide.isStart()) {
        int gridId = GeoUtils.matchGridCell(taxiRide.startLon, taxiRide.startLat);
        return new Tuple2<TaxiRide, Integer>(taxiRide, gridId);
      } else {
        int gridId = GeoUtils.matchGridCell(taxiRide.endLon, taxiRide.endLat);
        return new Tuple2<TaxiRide, Integer>(taxiRide, gridId);
      }
    }

  }

  /** KeySelector to combine girdId and event [start/end] as a key. */
  static class CustomKeySelector implements KeySelector<Tuple2<TaxiRide, Integer>, CustomTaxiRideKey> {
    @Override
    public CustomTaxiRideKey getKey(Tuple2<TaxiRide, Integer> input) throws Exception {
      return new CustomTaxiRideKey(input.f0.event, input.f1);
    }

  }

  /** POJO serving as combined key. */
  static class CustomTaxiRideKey {

    private TaxiRide.Event event;
    private Integer gridId;

    public CustomTaxiRideKey(TaxiRide.Event event, Integer gridId) {
      this.event = event;
      this.gridId = gridId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CustomTaxiRideKey test = (CustomTaxiRideKey) o;

      if (event != test.event) return false;
      return gridId.equals(test.gridId);
    }

    @Override
    public int hashCode() {
      int result = event.hashCode();
      result = 31 * result + gridId.hashCode();
      return result;
    }

  }

  /** WindowMapFunction to count starts or stops per grid cell. */
  static class PopularityCounter implements WindowMapFunction<Tuple2<TaxiRide,Integer>, Tuple2<Integer, Integer>> {
    @Override
    public void mapWindow(Iterable<Tuple2<TaxiRide, Integer>> values, Collector<Tuple2<Integer, Integer>> out)
            throws Exception {

      int gridId = 0;
      int counter = 0;
      for (Tuple2<TaxiRide, Integer> value : values) {
        gridId = value.f1;
        counter++;
      }

      out.collect(new Tuple2<Integer, Integer>(gridId, counter));
    }
  }

  /** FilterFunction to keep n most popular spots. */
  static class PopularityFilter implements FilterFunction<Tuple2<Integer, Integer>> {
    @Override
    public boolean filter(Tuple2<Integer, Integer> value) throws Exception {
      return value.f1 >= popularityIndex;
    }

  }

  /** Expects 3 arguments or none:
   *   1) path to input file
   *   2) fast-forward factor
   *   3) number of most popular grid cells
   *
   * @param args program arguments
   * @return true if arguments can be parsed, otherwise false
   */
  private static boolean parseArguments(String[] args) {
    if(args.length > 0) {
      if(args.length == 3) {
        inputPath = args[0];
        servingSpeedFactor = Float.parseFloat(args[1]);
        popularityIndex = Integer.parseInt(args[2]);
        return true;
      } else {
        return false;
      }
    }
    return true;
  }

}
