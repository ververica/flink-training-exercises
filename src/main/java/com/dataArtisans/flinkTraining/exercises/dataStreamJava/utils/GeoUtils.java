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

package com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils;

public class GeoUtils {

  /** Geo boundaries of the area of NYC */
  public static double LON_EAST = -73.7;
  public static double LON_WEST = -74.05;
  public static double LAT_NORTH = 41.0;
  public static double LAT_SOUTH = 40.5;

  /** delta step to create artificial gird overlay of NYC */
  public static double DELTA_LON = 0.0014;
  public static double DELTA_LAT = 0.00125;

  /** ( |LON_WEST| - |LON_EAST| ) / DELTA_LAT  */
  public static int NUMBER_OF_GRID_X = 250;
  /** ( LAT_NORTH - LAT_SOUTH ) / DELTA_LON */
  public static int NUMBER_OF_GRID_Y = 400;

  /** Checks if the start and stop location of the given TaxiRide are within the geo boundaries of NYC.
   *
   * @param taxiRide taxi ride to check
   * @return true if taxi ride starts / stops within NYC boundaries, otherwise false
   */
  public static boolean isInNYC(TaxiRide taxiRide) {
    if (taxiRide.startLon > LON_EAST || taxiRide.startLon < LON_WEST) {
      return false;
    } else if (taxiRide.endLon > LON_EAST || taxiRide.endLon < LON_WEST) {
      return false;
    } else if(taxiRide.startLat > LAT_NORTH || taxiRide.startLat < LAT_SOUTH) {
      return false;
    } else if(taxiRide.endLat > LAT_NORTH || taxiRide.endLat < LAT_SOUTH) {
      return false;
    }
    return true;
  }

  /** Computes the euclidean distance between the start- and endpoint of a taxi ride.
   *
   * @param start start of taxi ride
   * @param end end of taxi ride
   * @return the euclidean distance between start and end
   */
  public static float computeEuclideanDistance(TaxiRide start, TaxiRide end) {
    return (float)Math.sqrt(Math.pow(start.startLon - end.endLon, 2) + Math.pow(start.startLat - end.endLat, 2));
  }

  /** Matches the given latitude / longitude coordinates into a gird covering the area of NYC. The grid cells are
   * roughly 100 x 100 m and sequentially number from north-west to south-east starting by zero.
   *
   * @param lon longitude to match into grid
   * @param lat latitude to match into grid
   * @return index of matching grid cell
   */
  public static int matchGridCell(float lon, float lat) {
    int xIndex = (int)Math.floor((Math.abs(LON_WEST) - Math.abs(lon)) / DELTA_LON);
    int yIndex = (int)Math.floor((LAT_NORTH - lat) / DELTA_LAT);

    // TODO this is just for dev purpose
    if(xIndex > NUMBER_OF_GRID_X || yIndex > NUMBER_OF_GRID_Y) {
      throw new IllegalStateException("wrong index (" + xIndex + "/" + yIndex + ") " +
              "for coordinates (" + lon + "/" + lat + ")");
    }

    return xIndex + (yIndex * NUMBER_OF_GRID_X);
  }

}
