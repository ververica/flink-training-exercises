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

package com.dataArtisans.flinkTraining.mboxParser;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;

public class MBoxParsing {

	public static void main(String[] args) throws Exception {

		if(args.length != 2) {
			System.err.println("parameters <mbox-input> <mails-output>");
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> rawMails =
				env.readFile(new TextStringFormat("From dev-return-"), args[0]);

		rawMails
				.flatMap(new MBoxMailParser())
				.writeAsFormattedText(args[1], new MailFormatter());

		env.execute();
	}



	public static class TextStringFormat extends DelimitedInputFormat<String> {

		public TextStringFormat() {}

		public TextStringFormat(String stringDelimiter) {
			this.setDelimiter(stringDelimiter);
		}

		@Override
		public String readRecord(String reuse, byte[] bytes, int offset, int numBytes) throws IOException {
			return new String(bytes, offset, numBytes);
		}
	}

	public static class MBoxMailParser extends
			RichFlatMapFunction<String, Tuple6<DateTime, String, String, String, String, String>> {

		private transient DateTimeFormatter df;

		@Override
		public void open(Configuration config) {
			this.df = DateTimeFormat.forPattern("EEE MMM d HH:mm:ss yyyy");
		}

		@Override
		public void flatMap(String mail,
							Collector<Tuple6<DateTime, String, String, String, String, String>> out)
				throws Exception {

			boolean bodyStarted = false;
			StringBuilder bodyBuilder = null;

			DateTime time = null;
			String messageId = "null";
			String subject = null;
			String from = null;
			String replyTo = "null";
			String body = null;

			String[] lines = mail.split("\\n");
			if(lines.length > 0) {

				// check if mail is valid
				if(lines[0].length() < 24) {
					return;
				}
				// parse time
				String dateStr = lines[0].substring(lines[0].length()-24).replaceAll("\\s+", " ");
				time = DateTime.parse(dateStr, df);

				// skip first line
				for(int i=1; i<lines.length; i++) {
					String line = lines[i];
					if(line.startsWith("Subject: ")) {
						subject = line.substring(9);
					}
					else if(line.startsWith("From: ")) {
						from = line.substring(6);
					}
					else if(line.startsWith("In-Reply-To: ")) {
						replyTo = line.substring(13);
					}
					else if(line.startsWith("Message-Id: ")) {
						messageId = line.substring(12);
					}
					else if(line.startsWith("X-Virus-Checked: ")) {
						bodyStarted = true;
						bodyBuilder = new StringBuilder();
					}
					else if(bodyStarted) {

						// filter out forwarded/returned text
						if(!line.startsWith(">")) {
							if(line.endsWith("=")) {
								bodyBuilder.append(line, 0, line.length()-1);
							}
							else {
								bodyBuilder.append(line);
							}
						}
						else {
							if(line.endsWith("=")) {
								i++;
							}
						}
					}
				}
				if(bodyStarted) {
					body = bodyBuilder.toString();
				}
			}

			if(body != null) {

				out.collect(new Tuple6<DateTime, String, String, String, String, String>(
						time, from, subject, body, messageId, replyTo
				));

			}

		}
	}

	public static class MailFormatter implements TextOutputFormat.TextFormatter<Tuple6<DateTime, String, String, String, String, String>> {

		@Override
		public String format(Tuple6<DateTime, String, String, String, String, String> mail) {

			StringBuilder sb = new StringBuilder();

			// time
			sb.append(mail.f0.toString("yyyy-MM-dd-HH:mm:ss"));

			// subject
			sb.append("#|#");
			sb.append(mail.f1.replaceAll("#|#", "#!#"));

			// from
			sb.append("#|#");
			sb.append(mail.f2.replaceAll("#|#", "#!#"));

			// body
			sb.append("#|#");
			sb.append(mail.f3.replaceAll("#|#", "#!#"));

			// message ID
			sb.append("#|#");
			sb.append(mail.f4.replaceAll("#|#", "#!#"));

			// reply-to
			sb.append("#|#");
			sb.append(mail.f5.replaceAll("#|#", "#!#"));

			return sb.toString();
		}
	}

}
