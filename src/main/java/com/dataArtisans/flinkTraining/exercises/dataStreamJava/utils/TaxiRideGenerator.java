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

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Calendar;

// tripId,time,event[START|END],startLon,startLat,destLon,destLat,passengerCnt,tripDistance

public class TaxiRideGenerator implements SourceFunction<TaxiRide> {

  private String dataFilePath;
  private float servingSpeedFactor;

  private transient BufferedReader reader;

  private transient long dataStartTime;
  private transient long servingStartTime;

  public TaxiRideGenerator(String dataFilePath) {
    this(dataFilePath, 1.0f);
  }

  public TaxiRideGenerator(String dataFilePath, float servingSpeedFactor) {
    this.dataFilePath = dataFilePath;
    this.servingSpeedFactor = servingSpeedFactor;
  }

  @Override
  public void run(SourceContext<TaxiRide> sourceContext) throws Exception {

    this.servingStartTime = Calendar.getInstance().getTimeInMillis();
    reader = new BufferedReader(new FileReader(new File(dataFilePath)));

    String line;
    if (reader.ready() && (line = reader.readLine()) != null) {
      TaxiRide ride = TaxiRideUtils.deserialize(line);

      this.dataStartTime = ride.time.getMillis();
      sourceContext.collect(ride);
    } else {
      return;
    }

    while (reader.ready() && (line = reader.readLine()) != null) {

      TaxiRide ride = TaxiRideUtils.deserialize(line);

      long dataDiff = ride.time.getMillis() - this.dataStartTime;
      long servingDiff = Calendar.getInstance().getTimeInMillis() - this.servingStartTime;

      long wait = (long) ((dataDiff / this.servingSpeedFactor) - servingDiff);
      if (wait > 0) {
        Thread.sleep(wait);
      }

      sourceContext.collect(ride);
    }

    this.reader.close();
    this.reader = null;
  }

  @Override
  public void cancel() {
    try {
      if (this.reader != null) {
        this.reader.close();
      }
    } catch (IOException ioe) {
      //
    } finally {
      this.reader = null;
    }
  }

}

