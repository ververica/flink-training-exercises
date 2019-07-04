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

package com.ververica.flinktraining.examples.table_java.descriptors;

import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.util.Preconditions;

import java.util.Map;

import static com.ververica.flinktraining.examples.table_java.descriptors.TaxiRidesValidator.CONNECTOR_MAX_EVENT_DELAY_SECS;
import static com.ververica.flinktraining.examples.table_java.descriptors.TaxiRidesValidator.CONNECTOR_PATH;
import static com.ververica.flinktraining.examples.table_java.descriptors.TaxiRidesValidator.CONNECTOR_SERVING_SPEED_FACTOR;
import static com.ververica.flinktraining.examples.table_java.descriptors.TaxiRidesValidator.CONNECTOR_TYPE_VALUE_TAXI_RIDES;

/**
 * The taxi rides data as provided by the New York City Taxi & Limousine Commission.
 */
public class TaxiRides extends ConnectorDescriptor {

	public TaxiRides() {
		super(CONNECTOR_TYPE_VALUE_TAXI_RIDES, 1, false);
	}

	private String path;
	private Integer maxEventDelaySecs;
	private Integer servingSpeedFactor;

	public TaxiRides path(String path) {
		this.path = Preconditions.checkNotNull(path);
		return this;
	}

	public TaxiRides maxEventDelaySecs(int maxEventDelaySecs) {
		this.maxEventDelaySecs = maxEventDelaySecs;
		return this;
	}

	public TaxiRides servingSpeedFactor(int servingSpeedFactor) {
		this.servingSpeedFactor = servingSpeedFactor;
		return this;
	}

	@Override
	protected Map<String, String> toConnectorProperties() {
		DescriptorProperties properties = new DescriptorProperties();
		if (this.path != null) {
			properties.putString(CONNECTOR_PATH, this.path);
		}
		if (this.maxEventDelaySecs != null) {
			properties.putInt(CONNECTOR_MAX_EVENT_DELAY_SECS, this.maxEventDelaySecs);
		}
		if (this.servingSpeedFactor != null) {
			properties.putInt(CONNECTOR_SERVING_SPEED_FACTOR, this.servingSpeedFactor);
		}
		return properties.asMap();
	}
}
