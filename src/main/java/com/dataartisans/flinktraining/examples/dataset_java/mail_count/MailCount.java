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

package com.dataartisans.flinktraining.examples.dataset_java.mail_count;

import com.dataartisans.flinktraining.dataset_preparation.MBoxParser;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

/**
 * Java reference implementation for the "Mail Count" exercise of the Flink training.
 * The task of the exercise is to count the number of mails sent for each month and email address.
 *
 * Required parameters:
 *   --input path-to-input-directory
 *
 */
public class MailCount {

	public static void main(String[] args) throws Exception {

		// parse parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		String input = params.getRequired("input");

		// obtain an execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// read the "time" and "sender" fields of the input data set (field 2 and 3) as Strings
		DataSet<Tuple2<String, String>> mails =
			env.readCsvFile(input)
				.lineDelimiter(MBoxParser.MAIL_RECORD_DELIM)
				.fieldDelimiter(MBoxParser.MAIL_FIELD_DELIM)
				.includeFields("011")
				.types(String.class, String.class);

		mails
				// extract the month from the time field and the email address from the sender field
				.map(new MonthEmailExtractor())
				// group by month and email address and count number of records per group
				.groupBy(0, 1).reduceGroup(new MailCounter())
				// print the result
				.print();

	}

	/**
	 * Extracts the month from the time field and the email address from the sender field.
	 */
	public static class MonthEmailExtractor implements MapFunction<Tuple2<String, String>, Tuple2<String, String>> {

		@Override
		public Tuple2<String, String> map(Tuple2<String, String> mail) throws Exception {

			// extract year and month from time string
			String month = mail.f0.substring(0, 7);
			// extract email address from the sender
			String email = mail.f1.substring(mail.f1.lastIndexOf("<") + 1, mail.f1.length() - 1);

			return new Tuple2<>(month, email);
		}
	}

	/**
	 * Counts the number of mails per month and email address.
	 */
	public static class MailCounter implements GroupReduceFunction<Tuple2<String ,String>, Tuple3<String ,String, Integer>> {

		@Override
		public void reduce(Iterable<Tuple2<String, String>> mails, Collector<Tuple3<String, String, Integer>> out) throws Exception {

			String month = null;
			String email = null;
			int cnt = 0;

			// count number of tuples
			for(Tuple2<String, String> m : mails) {
				// remember month and email address
				month = m.f0;
				email = m.f1;
				// increase count
				cnt++;
			}

			// emit month, email address, and count
			out.collect(new Tuple3<>(month, email, cnt));
		}
	}

}
