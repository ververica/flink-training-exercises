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

package com.dataartisans.flinktraining.examples.table_java.batch.memberotm;

import com.dataartisans.flinktraining.dataset_preparation.MBoxParser;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Java reference implementation for the "Member of the Month" exercise of the Flink training.
 * The task of the exercise is to identify for each month the email address that sent the most
 * emails to the Flink developer mailing list.
 *
 * Required parameters:
 * --input path-to-input-directory
 *
 */
public class MemberOTMonth {

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

		DataSet<Tuple2<String, String>> monthSender = mails
				// extract the month from the time field and the email address from the sender field
				.map(new MonthEmailExtractor());

		BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

		Table mailsPerSenderMonth = tEnv
				// to table
				.fromDataSet(monthSender, "month, sender")
				// filter out bot email addresses
				.filter("sender !== 'jira@apache.org' && " +
						"sender !== 'no-reply@apache.org' && " +
						"sender !== 'git@git.apache.org'")
				// count emails per month and email address
				.groupBy("month, sender").select("month, sender, month.count as cnt");

		Table membersOTMonth = mailsPerSenderMonth
				// find max number of emails sent by an address per month
				.groupBy("month").select("month as m, cnt.max as max")
				// find email address that sent the most emails in each month
				.join(mailsPerSenderMonth).where("month = m && cnt = max").select("month, sender");

		// print out result
		tEnv.toDataSet(membersOTMonth, Row.class).print();

	}

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

}
