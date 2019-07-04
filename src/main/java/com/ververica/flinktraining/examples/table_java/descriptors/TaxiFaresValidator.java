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

import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

/**
 * Represents a validator for {@link TaxiFares}.
 */
public class TaxiFaresValidator extends ConnectorDescriptorValidator {
	public static final String CONNECTOR_TYPE_VALUE_TAXI_FARES = "taxi-fares";
	public static final String CONNECTOR_PATH = "connector.path";
	public static final String CONNECTOR_MAX_EVENT_DELAY_SECS = "connector.max-event-delay-secs";
	public static final String CONNECTOR_SERVING_SPEED_FACTOR = "connector.serving-speed-factor";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateValue(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_TAXI_FARES, false);
		properties.validateString(CONNECTOR_PATH, false);
		properties.validateInt(CONNECTOR_MAX_EVENT_DELAY_SECS, true, 0);
		properties.validateInt(CONNECTOR_SERVING_SPEED_FACTOR, true, 1);
	}
}
