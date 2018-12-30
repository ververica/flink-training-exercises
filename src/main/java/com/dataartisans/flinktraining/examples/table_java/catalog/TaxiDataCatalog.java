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

package com.dataartisans.flinktraining.examples.table_java.catalog;

import com.dataartisans.flinktraining.examples.table_java.descriptors.TaxiFares;
import com.dataartisans.flinktraining.examples.table_java.descriptors.TaxiRides;
import com.dataartisans.flinktraining.examples.table_java.sources.TaxiRideTableSource;
import com.dataartisans.flinktraining.examples.table_java.sources.TaxiFareTableSource;
import org.apache.flink.table.api.CatalogNotExistException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.catalog.ExternalCatalog;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A catalog of taxi data as provided by the New York City Taxi & Limousine Commission.
 * <p>
 * The following tables are provided:
 * - TaxiRides - taxi rides by {@link TaxiRideTableSource}
 * - TaxiFares - taxi fares by {@link TaxiFareTableSource}
 */
public class TaxiDataCatalog implements ExternalCatalog {

	private static final String CATALOG_NAME_DEFAULT = "nyc";

	private Map<String, ExternalCatalogTable> tables;

	/**
	 * Creates a taxi data catalog.
	 *
	 * @param ridesFile          the gzipped input file from which the taxi ride rows are read, or null to skip the TaxiRides table.
	 * @param faresFile          the gzipped input file from which the taxi fare rows are read, or null to skip the TaxiFares table.
	 * @param maxEventDelay      the max time in seconds by which rows are delayed.
	 * @param servingSpeedFactor the serving speed factor by which the logical serving time is adjusted.
	 */
	public TaxiDataCatalog(String ridesFile, String faresFile, int maxEventDelay, int servingSpeedFactor) {
		Preconditions.checkArgument(maxEventDelay >= 0);
		Preconditions.checkArgument(servingSpeedFactor >= 1);
		this.tables = new LinkedHashMap<>();

		if (ridesFile != null) {
			// TaxiRides
			TaxiRides ridesDescriptor = new TaxiRides()
					.path(ridesFile)
					.maxEventDelaySecs(maxEventDelay)
					.servingSpeedFactor(servingSpeedFactor);
			this.tables.put(
					"TaxiRides",
					ExternalCatalogTable.builder(ridesDescriptor)
							.supportsStreaming()
							.inAppendMode()
							.asTableSource());
		}

		if (faresFile != null) {
			// TaxiFares
			TaxiFares faresDescriptor = new TaxiFares()
					.path(faresFile)
					.maxEventDelaySecs(maxEventDelay)
					.servingSpeedFactor(servingSpeedFactor);
			this.tables.put(
					"TaxiFares",
					ExternalCatalogTable.builder(faresDescriptor)
							.supportsStreaming()
							.inAppendMode()
							.asTableSource());
		}
	}

	@Override
	public ExternalCatalogTable getTable(String tableName) throws TableNotExistException {
		if (!tables.containsKey(tableName)) {
			throw new TableNotExistException(CATALOG_NAME_DEFAULT, tableName);
		}
		return tables.get(tableName);
	}

	@Override
	public List<String> listTables() {
		return new ArrayList<>(tables.keySet());
	}

	@Override
	public ExternalCatalog getSubCatalog(String dbName) throws CatalogNotExistException {
		throw new CatalogNotExistException(dbName);
	}

	@Override
	public List<String> listSubCatalogs() {
		return Collections.emptyList();
	}
}
