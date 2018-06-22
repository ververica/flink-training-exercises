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

package com.dataartisans.flinktraining.examples.dataset_java.reply_graph;

import com.dataartisans.flinktraining.dataset_preparation.MBoxParser;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

/**
 * Java reference implementation for the "Reply Graph" exercise of the Flink training.
 * The task of the exercise is to enumerate the reply connection between two email addresses in
 * Flink's developer mailing list and count the number of connections between two email addresses.
 *
 * Required parameters:
 *   --input path-to-input-directory
 */
public class ReplyGraph {

	public static void main(String[] args) throws Exception {

		// parse parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		String input = params.getRequired("input");

		// obtain an execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// read messageId, sender, and reply-to fields from the input data set
		DataSet<Tuple3<String, String, String>> mails =
				env.readCsvFile(input)
						.lineDelimiter(MBoxParser.MAIL_RECORD_DELIM)
						.fieldDelimiter(MBoxParser.MAIL_FIELD_DELIM)
						// messageId at position 0, sender at 2, reply-to at 5
						.includeFields("101001")
						.types(String.class, String.class, String.class);

		// extract email addresses and filter out mails from bots
		DataSet<Tuple3<String, String, String>> addressMails = mails
				.map(new EmailExtractor())
				.filter(new ExcludeEmailFilter("git@git.apache.org"))
				.filter(new ExcludeEmailFilter("jira@apache.org"));

		// construct reply connections by joining on messageId and reply-To
		DataSet<Tuple2<String, String>> replyConnections = addressMails
				.join(addressMails).where(2).equalTo(0).projectFirst(1).projectSecond(1);

		// count reply connections for each pair of email addresses
		replyConnections
				.groupBy(0, 1).reduceGroup(new ConnectionCounter())
				.print();

	}

	/**
	 * Extracts the email address from the sender field
	 */
	public static class EmailExtractor implements MapFunction<Tuple3<String, String, String>, Tuple3<String, String, String>> {

		@Override
		public Tuple3<String, String, String> map(Tuple3<String, String, String> mail) throws Exception {
			mail.f1 = mail.f1.substring(mail.f1.lastIndexOf("<") + 1, mail.f1.length() - 1);
			return mail;
		}
	}

	/**
	 * Filter records for a specific email address
	 */
	public static class ExcludeEmailFilter implements FilterFunction<Tuple3<String, String, String>> {

		private String filterEmail;

		public ExcludeEmailFilter() {}

		public ExcludeEmailFilter(String filterMail) {
			this.filterEmail = filterMail;
		}

		@Override
		public boolean filter(Tuple3<String, String, String> mail) throws Exception {
			return !mail.f1.equals(filterEmail);
		}
	}

	/**
	 * Count the number of records per group
	 */
	public static class ConnectionCounter implements GroupReduceFunction<Tuple2<String,String>, Tuple3<String, String, Integer>> {

		Tuple3<String, String, Integer> outT = new Tuple3<>();

		@Override
		public void reduce(Iterable<Tuple2<String, String>> cs, Collector<Tuple3<String, String, Integer>> out) {
			outT.f2 = 0;

			for(Tuple2<String, String> c : cs) {
				outT.f0 = c.f0;
				outT.f1 = c.f1;
				outT.f2 += 1;
			}
			out.collect(outT);
		}
	}

}
