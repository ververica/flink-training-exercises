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

package com.dataArtisans.flinkTraining.exercise.mailCount;

import com.dataArtisans.flinkTraining.preprocess.MBoxParser;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class MailCount {

	public static void main(String[] args) throws Exception {

		if(args.length != 1) {
			System.err.println("parameters: <mails-input>");
			System.exit(1);
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<String, String>> mails =
			env.readCsvFile(args[0])
				.lineDelimiter(MBoxParser.MAIL_RECORD_DELIM)
				.fieldDelimiter(MBoxParser.MAIL_FIELD_DELIM)
				.types(String.class, String.class);

		mails
				.map(new MonthEmailExtractor())
				.groupBy(0, 1).reduceGroup(new MailCounter())
				.print();

		env.execute();

	}

	public static class MonthEmailExtractor implements MapFunction<Tuple2<String, String>, Tuple2<String, String>> {

		@Override
		public Tuple2<String, String> map(Tuple2<String, String> mail) throws Exception {

			// extract year and month from dateTime string
			String month = mail.f0.substring(0, 7);
			// extract email address
			String email = mail.f1.substring(mail.f1.lastIndexOf("<") + 1, mail.f1.length() - 1);

			return new Tuple2<String, String>(month, email);
		}
	}

	public static class MailCounter implements GroupReduceFunction<Tuple2<String ,String>, Tuple3<String ,String, Integer>> {

		@Override
		public void reduce(Iterable<Tuple2<String, String>> mails, Collector<Tuple3<String, String, Integer>> out) throws Exception {

			String month = null;
			String email = null;
			int cnt = 0;

			// count number of tuples
			for(Tuple2<String, String> m : mails) {
				month = m.f0;
				email = m.f1;
				cnt++;
			}

			// emit tuple with count
			out.collect(new Tuple3<String, String, Integer>(month, email, cnt));
		}
	}


}
