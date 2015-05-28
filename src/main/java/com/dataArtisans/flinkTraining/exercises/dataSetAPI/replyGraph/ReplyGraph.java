/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataArtisans.flinkTraining.exercises.dataSetAPI.replyGraph;

import com.dataArtisans.flinkTraining.dataSetPreparation.MBoxParser;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class ReplyGraph {

	public static void main(String[] args) throws Exception {

//		if(args.length != 1) {
//			System.err.println("parameters: <mails-input>");
//			System.exit(1);
//		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<String, String, String>> mails =
				env.readCsvFile("/users/fhueske/data/FlinkDevListParsed")
						.lineDelimiter(MBoxParser.MAIL_RECORD_DELIM)
						.fieldDelimiter(MBoxParser.MAIL_FIELD_DELIM)
						.includeFields("101001")
						.types(String.class, String.class, String.class);

		DataSet<Tuple3<String, String, String>> addressMails = mails
				.map(new EmailExtractor())
				.filter(new ExcludeEmailFilter("git@git.apache.org"))
				.filter(new ExcludeEmailFilter("jira@apache.org"));


		DataSet<Tuple2<String, String>> replyConnections = addressMails
				.join(addressMails).where(2).equalTo(0).projectFirst(1).<Tuple2<String, String>>projectSecond(1);

		replyConnections.map(
				new MapFunction<Tuple2<String, String>, Tuple3<String, String, Integer>>() {

					@Override
					public Tuple3<String, String, Integer> map(Tuple2<String, String> c) throws Exception {
						return new Tuple3<String, String, Integer>(c.f0, c.f1, 1);
					}
				})
				.groupBy(0, 1).sum(2)
				.sortPartition(2, Order.DESCENDING).setParallelism(1)
				.print();

	}

	public static class EmailExtractor implements MapFunction<Tuple3<String, String, String>, Tuple3<String, String, String>> {

		@Override
		public Tuple3<String, String, String> map(Tuple3<String, String, String> mail) throws Exception {
			String email = mail.f1.substring(mail.f1.lastIndexOf("<") + 1, mail.f1.length() - 1);
			mail.f1 = email;
			return mail;
		}
	}

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

}
