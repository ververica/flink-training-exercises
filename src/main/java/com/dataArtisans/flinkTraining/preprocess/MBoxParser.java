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

package com.dataArtisans.flinkTraining.preprocess;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;

public class MBoxParser {

	public static String MAIL_FIELD_DELIM = "#|#";
	public static String MAIL_RECORD_DELIM = "##//##";

	public static void main(String[] args) throws Exception {

		if(args.length != 3) {
			System.err.println("parameters <mbox-input> <mailer-daemon-prefix> <mails-output>");
			System.exit(1);
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> rawMails =
				env.readFile(new MBoxMailFormat(args[1]), args[0]);

		DataSet<Tuple6<String, String, String, String, String, String>> mails = rawMails
				.flatMap(new MBoxMailParser(args[1]))
				.distinct(0);

		mails.writeAsCsv(args[2], MAIL_RECORD_DELIM, MAIL_FIELD_DELIM);
		env.execute();

	}

	public static class MBoxMailFormat extends DelimitedInputFormat<String> {

		String newMailPrefix;

		public MBoxMailFormat() {};

		public MBoxMailFormat(String mailDaemonPrefix) {
			this.setDelimiter("From "+mailDaemonPrefix);
			this.newMailPrefix = "From "+mailDaemonPrefix;
		}

		@Override
		public String readRecord(String reuse, byte[] bytes, int offset, int numBytes) throws IOException {

			String textString = new String(bytes, offset, numBytes);
			if(textString.startsWith("From ")) {
				return textString;
			}
			else {
				return newMailPrefix + textString;
			}
		}
	}

	public static class MBoxMailParser extends
			RichFlatMapFunction<String, Tuple6<String, String, String, String, String, String>> {

		private String newMailPrefix;
		private transient DateTimeFormatter inDF;
		private transient DateTimeFormatter outDF;

		public MBoxMailParser() {};

		public MBoxMailParser(String mailDaemonPrefix) {
			this.newMailPrefix = "From "+mailDaemonPrefix;
		}

		@Override
		public void open(Configuration config) {
			this.inDF = DateTimeFormat.forPattern("EEE MMM d HH:mm:ss yyyy").withZoneUTC();
			this.outDF = DateTimeFormat.forPattern("yyyy-MM-dd-HH:mm:ss").withZoneUTC();
		}

		@Override
		public void flatMap(String mail,
							Collector<Tuple6<String, String, String, String, String, String>> out)
				throws Exception {

			boolean bodyStarted = false;
			StringBuilder bodyBuilder = null;

			String time = null;
			String from = null;
			String subject = null;
			String body = null;
			String messageId = "null";
			String replyTo = "null";

			String[] lines = mail.split("\\n");

			for(int i=0; i<lines.length; i++) {
				String line = lines[i];

				// body starts with first empty line
				if(!bodyStarted && line.trim().length() == 0) {
					bodyStarted = true;
					bodyBuilder = new StringBuilder();
				}

				if(bodyStarted) {
					// '=' indicates a wrapped line and should be removed
					if(line.endsWith("=")) {
						bodyBuilder.append(line, 0, line.length()-1);
					}
					else {
						bodyBuilder.append(line);
						bodyBuilder.append('\n');
					}
				}
				else if(line.startsWith(newMailPrefix)) {
					if(line.length() < 24) {
						return;
					}
					// parse time
					String dateStr = line.substring(line.length() - 24).replaceAll("\\s+", " ");
					time = DateTime.parse(dateStr, inDF).toString(outDF);
				}
				else if(line.toLowerCase().startsWith("subject: ")) {
					subject = line.substring(9);
					if(containsDelimiter(subject)) {
						// don't include mail
						return;
					}
				}
				else if(line.toLowerCase().startsWith("from: ")) {
					from = line.substring(6);
					if(containsDelimiter(from)) {
						// don't include mail
						return;
					}
				}
				else if(line.toLowerCase().startsWith("message-id: ")) {
					messageId = line.substring(12);
					if(containsDelimiter(messageId)) {
						// don't include mail
						return;
					}
				}
				else if(line.toLowerCase().startsWith("in-reply-to: ")) {
					replyTo = line.substring(13);
					if(containsDelimiter(replyTo)) {
						// don't include mail
						return;
					}
				}
			}

			if(messageId != null && time != null && from != null && subject != null && bodyStarted) {
				body = bodyBuilder.toString();
				if(containsDelimiter(body)) {
					// don't include email
					return;
				}

				out.collect(new Tuple6<String, String, String, String, String, String>(
						messageId, time, from, subject, body, replyTo
				));

			}

		}

		private boolean containsDelimiter(String s) {
			return s.contains(MAIL_FIELD_DELIM) || s.contains(MAIL_RECORD_DELIM);
		}

	}

}
