/*
 * Copyright 2019 Ververica GmbH
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

package com.ververica.flinktraining.examples.datastream_java.basics;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import com.ververica.flinktraining.examples.datastream_java.basics.utils.ParallelCollectionSource;
import com.ververica.flinktraining.examples.datastream_java.basics.utils.SinkCollectingLongs;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class TestableStreamingJobTest {
	@ClassRule
	public static MiniClusterWithClientResource flinkCluster =
			new MiniClusterWithClientResource(
					new MiniClusterResourceConfiguration.Builder()
							.setNumberSlotsPerTaskManager(2)
							.setNumberTaskManagers(1)
							.build());

	@Test
	public void testCompletePipeline() throws Exception {

		// Arrange
		ParallelSourceFunction<Long> source =
				new ParallelCollectionSource(Arrays.asList(1L, 10L, -10L));
		SinkCollectingLongs sink = new SinkCollectingLongs();
		TestableStreamingJob job = new TestableStreamingJob(source, sink);

		// Act
		job.execute();

		// Assert
		assertThat(sink.result).containsExactlyInAnyOrder(2L, 11L, -9L);
	}
}
