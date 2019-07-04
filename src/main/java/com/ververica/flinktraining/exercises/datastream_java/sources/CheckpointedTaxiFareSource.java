/*
 * Copyright 2015 data Artisans GmbH, 2019 Ververica GmbH
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

package com.ververica.flinktraining.exercises.datastream_java.sources;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * This SourceFunction generates a data stream of TaxiFare records which are
 * read from a gzipped input file. Each record has a time stamp and the input file must be
 * ordered by this time stamp.
 *
 * In order to simulate a realistic stream source, the SourceFunction serves events proportional to
 * their timestamps.
 *
 * The serving speed of the SourceFunction can be adjusted by a serving speed factor.
 * A factor of 60.0 increases the logical serving time by a factor of 60, i.e., events of one
 * minute (60 seconds) are served in 1 second.
 *
 * This SourceFunction is an EventSourceFunction and does continuously emit watermarks.
 * Hence it is able to operate in event time mode which is configured as follows:
 *
 *   StreamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
 *
 * In addition it implements the Checkpointed interface and can hence able to recover from
 * failures if the job enables checkpointing as follows:
 *
 *   StreamExecutionEnvironment.enableCheckpointing(Long)
 *
 */
public class CheckpointedTaxiFareSource implements SourceFunction<TaxiFare>, ListCheckpointed<Long> {

	private final String dataFilePath;
	private final int servingSpeed;

	private transient BufferedReader reader;
	private transient InputStream gzipStream;

	// state
	// number of emitted events
	private long eventCnt = 0;

	/**
	 * Serves the TaxiFare records from the specified and ordered gzipped input file.
	 * Rides are served out-of time stamp order with specified maximum random delay
	 * in a serving speed which is proportional to the specified serving speed factor.
	 *
	 * @param dataFilePath The gzipped input file from which the TaxiFare records are read.
	 */
	public CheckpointedTaxiFareSource(String dataFilePath) {
		this(dataFilePath, 1);
	}

	/**
	 * Serves the TaxiFare records from the specified and ordered gzipped input file.
	 * Rides are served exactly in order of their time stamps
	 * in a serving speed which is proportional to the specified serving speed factor.
	 *
	 * @param dataFilePath The gzipped input file from which the TaxiFare records are read.
	 * @param servingSpeedFactor The serving speed factor by which the logical serving time is adjusted.
	 */
	public CheckpointedTaxiFareSource(String dataFilePath, int servingSpeedFactor) {
		this.dataFilePath = dataFilePath;
		this.servingSpeed = servingSpeedFactor;
	}

	@Override
	public void run(SourceContext<TaxiFare> sourceContext) throws Exception {

		final Object lock = sourceContext.getCheckpointLock();

		gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath));
		reader = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"));

		Long prevRideTime = null;

		String line;
		long cnt = 0;

		// skip emitted events
		while (cnt < eventCnt && reader.ready() && (line = reader.readLine()) != null) {
			cnt++;
			TaxiFare fare = TaxiFare.fromString(line);
			prevRideTime = getEventTime(fare);
		}

		// emit all subsequent events proportial to their timestamp
		while (reader.ready() && (line = reader.readLine()) != null) {

			TaxiFare fare = TaxiFare.fromString(line);
			long rideTime = getEventTime(fare);

			if (prevRideTime != null) {
				long diff = (rideTime - prevRideTime) / servingSpeed;
				Thread.sleep(diff);
			}

			synchronized (lock) {
				eventCnt++;
				sourceContext.collectWithTimestamp(fare, rideTime);
				sourceContext.emitWatermark(new Watermark(rideTime - 1));
			}

			prevRideTime = rideTime;
		}

		this.reader.close();
		this.reader = null;
		this.gzipStream.close();
		this.gzipStream = null;

	}

	public long getEventTime(TaxiFare fare) {
		return fare.getEventTime();
	}

	@Override
	public void cancel() {
		try {
			if (this.reader != null) {
				this.reader.close();
			}
			if (this.gzipStream != null) {
				this.gzipStream.close();
			}
		} catch(IOException ioe) {
			throw new RuntimeException("Could not cancel SourceFunction", ioe);
		} finally {
			this.reader = null;
			this.gzipStream = null;
		}
	}

	@Override
	public List<Long> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
		return Collections.singletonList(eventCnt);
	}

	@Override
	public void restoreState(List<Long> state) throws Exception {
		for (Long s : state)
			this.eventCnt = s;
	}
}
