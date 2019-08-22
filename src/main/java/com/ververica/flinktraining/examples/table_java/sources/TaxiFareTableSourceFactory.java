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

package com.ververica.flinktraining.examples.table_java.sources;

import com.ververica.flinktraining.examples.table_java.descriptors.TaxiFaresValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.StreamTableDescriptorValidator;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ververica.flinktraining.examples.table_java.descriptors.TaxiFaresValidator.CONNECTOR_MAX_EVENT_DELAY_SECS;
import static com.ververica.flinktraining.examples.table_java.descriptors.TaxiFaresValidator.CONNECTOR_PATH;
import static com.ververica.flinktraining.examples.table_java.descriptors.TaxiFaresValidator.CONNECTOR_PROPERTY_VERSION;
import static com.ververica.flinktraining.examples.table_java.descriptors.TaxiFaresValidator.CONNECTOR_SERVING_SPEED_FACTOR;
import static com.ververica.flinktraining.examples.table_java.descriptors.TaxiFaresValidator.CONNECTOR_TYPE;
import static com.ververica.flinktraining.examples.table_java.descriptors.TaxiFaresValidator.CONNECTOR_TYPE_VALUE_TAXI_FARES;

/**
 * A table factory for {@link TaxiFareTableSource}.
 */
public class TaxiFareTableSourceFactory implements StreamTableSourceFactory<Row> {

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_TAXI_FARES); // taxi-fares
		context.put(CONNECTOR_PROPERTY_VERSION, "1"); // backwards compatibility
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();
		properties.add(StreamTableDescriptorValidator.UPDATE_MODE);
		properties.add(CONNECTOR_PATH);
		properties.add(CONNECTOR_MAX_EVENT_DELAY_SECS);
		properties.add(CONNECTOR_SERVING_SPEED_FACTOR);
		return properties;
	}

	@Override
	public TaxiFareTableSource createStreamTableSource(Map<String, String> properties) {
		DescriptorProperties params = getValidatedProperties(properties);
		return new TaxiFareTableSource(
				params.getString(CONNECTOR_PATH),
				params.getOptionalInt(CONNECTOR_MAX_EVENT_DELAY_SECS).orElse(0),
				params.getOptionalInt(CONNECTOR_SERVING_SPEED_FACTOR).orElse(1)
		);
	}

	private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);

		new StreamTableDescriptorValidator(true, false, false).validate(descriptorProperties);
		new TaxiFaresValidator().validate(descriptorProperties);

		return descriptorProperties;
	}
}
