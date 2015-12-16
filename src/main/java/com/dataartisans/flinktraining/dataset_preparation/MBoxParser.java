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

package com.dataartisans.flinktraining.dataset_preparation;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Locale;
import java.io.IOException;

/**
 * Converts MBox archive files of Apache Flink's developer mailing list into a processable
 * text-based, delimited file format.
 * For MBox files of different mailing lists, the MAIL_DAEMON_PREFIX parameter needs to be adapted.
 *
 * Mail records are separated by "##//##" and record fields are separated by "#|#".
 *
 * Mail records have six fields:
 * - MessageID : String (unique)
 * - Time      : String
 * - Sender    : String
 * - Subject   : String
 * - Body      : String
 * - Reply-To  : String (points a MessageID, might be "null")
 *
 * Parameters:
 *   --input path-to-mbox-files
 *   --output path-to-output
 *
 */
public class MBoxParser {

	public final static String MAIL_FIELD_DELIM = "#|#";
	public final static String MAIL_RECORD_DELIM = "##//##";

	private final static String MAIL_DAEMON_PREFIX = "dev-return";

	public static void main(String[] args) throws Exception {

		// parse parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		String input = params.getRequired("input");
		String output = params.getRequired("output");

		// obtain execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// read the raw mail data, each mail is a String
		DataSet<String> rawMails =
				env.readFile(new MBoxMailFormat(MAIL_DAEMON_PREFIX), input);

		// parse mail String into record with six fields
		DataSet<Tuple6<String, String, String, String, String, String>> mails = rawMails
				// parse mails
				.flatMap(new MBoxMailParser(MAIL_DAEMON_PREFIX))
				// filter out mails with duplicate messageIds
				.distinct(0);

		// write mail records as delimited files
		mails.writeAsCsv(output, MAIL_RECORD_DELIM, MAIL_FIELD_DELIM);
		env.execute();
	}

	/**
	 * InputFormat that splits MBox files into strings of individual mails.
	 * Each mail becomes a String.
	 */
	public static class MBoxMailFormat extends DelimitedInputFormat<String> {

		String newMailPrefix;

		public MBoxMailFormat() {}

		public MBoxMailFormat(String mailDaemonPrefix) {
			// set the record delimiter
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
				// append the delimiter if it was cut off
				return newMailPrefix + textString;
			}
		}
	}

	/**
	 * Parses a mail String into a record of six fields: (MessageId, Time, Sender, Subject, Body, Reply-To).
	 */
	public static class MBoxMailParser extends
			RichFlatMapFunction<String, Tuple6<String, String, String, String, String, String>> {

		private String newMailPrefix;
		private transient DateTimeFormatter inDF;
		private transient DateTimeFormatter outDF;

		public MBoxMailParser() {}

		public MBoxMailParser(String mailDaemonPrefix) {
			this.newMailPrefix = "From "+mailDaemonPrefix;
		}

		@Override
		public void open(Configuration config) {
			// configure DataTime formats for parsing and formatting
			this.inDF = DateTimeFormat.forPattern("EEE MMM d HH:mm:ss yyyy").withLocale(Locale.US).withZoneUTC();
			this.outDF = DateTimeFormat.forPattern("yyyy-MM-dd-HH:mm:ss").withLocale(Locale.US).withZoneUTC();
		}

		@Override
		public void flatMap(String mail, Collector<Tuple6<String, String, String, String, String, String>> out) {

			boolean bodyStarted = false;
			StringBuilder bodyBuilder = null;

			String time = null;
			String from = null;
			String subject = null;
			String messageId = null;
			String replyTo = "null";

			// split mail String line-wise
			String[] lines = mail.split("\\n");

			for (String line : lines) {
				// body starts with first empty line
				if(!bodyStarted && line.trim().length() == 0) {
					bodyStarted = true;
					bodyBuilder = new StringBuilder();
					// all following lines are added to the body
				}

				if(bodyStarted) {
					// '=' indicates a wrapped line and should be removed
					if(line.endsWith("=")) {
						bodyBuilder.append(line, 0, line.length()-1);
					}
					else {
						bodyBuilder.append(line).append('\n');
					}
				}
				// first line of a mail, contains time
				else if(line.startsWith(newMailPrefix)) {
					if(line.length() < 24) {
						return;
					}
					// parse time
					String dateStr = line.substring(line.length() - 24).replaceAll("\\s+", " ");
					time = DateTime.parse(dateStr, inDF).toString(outDF);
				}
				// extract subject
				else if(line.toLowerCase().startsWith("subject: ")) {
					subject = line.substring(9);
					if(containsDelimiter(subject)) {
						// don't include mail
						return;
					}
				}
				// extract sender
				else if(line.toLowerCase().startsWith("from: ")) {
					from = line.substring(6);
					if(containsDelimiter(from)) {
						// don't include mail
						return;
					}
				}
				// extract message-id
				else if(line.toLowerCase().startsWith("message-id: ")) {
					messageId = line.substring(12);
					if(containsDelimiter(messageId)) {
						// don't include mail
						return;
					}
				}
				// extract reply-to
				else if(line.toLowerCase().startsWith("in-reply-to: ")) {
					replyTo = line.substring(13);
					if(containsDelimiter(replyTo)) {
						// don't include mail
						return;
					}
				}
			}

			// check if all fields (except reply to) are set
			if(messageId != null && time != null && from != null && subject != null && bodyStarted) {
				String body = bodyBuilder.toString();
				if(containsDelimiter(body)) {
					// don't include email
					return;
				}

				out.collect(new Tuple6<>(messageId, time, from, subject, body, replyTo));
			}

		}

		/**
		 * Checks if the string contains a record or field delimiter.
		 *
		 * @param s The string to check.
		 * @return True if a delimiter is contained, false otherwise.
		 */
		private boolean containsDelimiter(String s) {
			return s.contains(MAIL_FIELD_DELIM) || s.contains(MAIL_RECORD_DELIM);
		}

	}

}
