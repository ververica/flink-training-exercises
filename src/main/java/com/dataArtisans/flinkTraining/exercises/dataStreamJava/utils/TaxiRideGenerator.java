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

package com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils;

import com.dataArtisans.flinkTraining.exercises.dataStreamJava.dataTypes.TaxiRide;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.zip.GZIPInputStream;

/**
 *
 */
public class TaxiRideGenerator implements SourceFunction<TaxiRide> {

	private String dataFilePath;
	private float servingSpeedFactor;

	private transient BufferedReader reader;
	private transient InputStream gzipStream;

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

		gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath));
		reader = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"));

		String line;
		if (reader.ready() && (line = reader.readLine()) != null) {
			TaxiRide ride = TaxiRide.fromString(line);

			this.dataStartTime = ride.time.getMillis();
			sourceContext.collect(ride);
		} else {
			return;
		}

		while (reader.ready() && (line = reader.readLine()) != null) {

			TaxiRide ride = TaxiRide.fromString(line);

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
		this.gzipStream.close();
		this.gzipStream = null;
	}

	@Override
	public void cancel() {
		try {
			if (this.reader != null) {
				this.reader.close();
			}
			if( this.gzipStream != null) {
				this.gzipStream.close();
			}
		} catch (IOException ioe) {
			//
		} finally {
			this.reader = null;
			this.gzipStream = null;
		}
	}

}

