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

package com.dataArtisans.flinkTraining.tfIdf;

import com.dataArtisans.flinkTraining.mboxParser.MBoxParser;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MailTFIDF {

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
						.types(String.class, String.class, String.class)
				.map(new DocIdGenerator());

		long docCount = mails.count();

		DataSet<String> stopWords = env.fromElements("the", "i", "a", "an", "at", "are", "am", "for",
				"there", "it", "this", "that", "on", "was", "by", "of", "to", "in", "and", "or", "is",
				"to", "message", "not", "be", "with", "you", "have", "as", "can");

		DataSet<Tuple2<String, Integer>> docFrequency = mails
				.flatMap(new UniqueWordExtractor()).withBroadcastSet(stopWords, "stopWords")
				.groupBy(0).sum(1);

		DataSet<Tuple3<String, String, Integer>> termFrequency = mails
				.flatMap(new TFComputer()).withBroadcastSet(stopWords, "stopWords");

		DataSet<Tuple3<String, String, Double>> tfIdf =
				docFrequency.join(termFrequency).where(0).equalTo(1)
					.with(new TfIdfComputer(docCount));

		tfIdf.print();
		env.execute();

	}

	public static class DocIdGenerator implements MapFunction<Tuple3<String, String, String>, Tuple2<String, String>> {

		@Override
		public Tuple2<String, String> map(Tuple3<String, String, String> mail) throws Exception {
			return new Tuple2<String, String>(mail.f0 + mail.f1, mail.f2);
		}
	}

	public static class UniqueWordExtractor extends RichFlatMapFunction<Tuple2<String, String>, Tuple2<String, Integer>> {

		private transient Set<String> stopWords;
		private transient Set<String> emittedWords;
		private transient Pattern wordPattern;

		@Override
		public void open(Configuration config) {
			this.stopWords = new HashSet<String>(this.getRuntimeContext().<String>getBroadcastVariable("stopWords"));
			this.emittedWords = new HashSet<String>();
			this.wordPattern = Pattern.compile("(\\p{Alpha})+");
		}

		@Override
		public void flatMap(Tuple2<String, String> mail, Collector<Tuple2<String, Integer>> out) throws Exception {

			this.emittedWords.clear();
			StringTokenizer st = new StringTokenizer(mail.f1);

			while(st.hasMoreTokens()) {
				String word = st.nextToken().toLowerCase();
				Matcher m = this.wordPattern.matcher(word);
				if(m.matches() && !this.stopWords.contains(word) && !this.emittedWords.contains(word)) {
					out.collect(new Tuple2<String, Integer>(word, 1));
					this.emittedWords.add(word);
				}
			}
		}
	}

	public static class TFComputer extends RichFlatMapFunction<Tuple2<String, String>, Tuple3<String, String, Integer>> {

		private transient Set<String> stopWords;
		private transient Map<String, Integer> wordCounts;
		private transient Pattern wordPattern;

		@Override
		public void open(Configuration config) {
			this.stopWords = new HashSet(this.getRuntimeContext().<String>getBroadcastVariable("stopWords"));
			this.wordPattern = Pattern.compile("(\\p{Alpha})+");
			this.wordCounts = new HashMap<String, Integer>();
		}

		@Override
		public void flatMap(Tuple2<String, String> mail, Collector<Tuple3<String, String, Integer>> out) throws Exception {

			this.wordCounts.clear();

			StringTokenizer st = new StringTokenizer(mail.f1);
			while(st.hasMoreTokens()) {
				String word = st.nextToken().toLowerCase();
				Matcher m = this.wordPattern.matcher(word);
				if(m.matches() && !this.stopWords.contains(word)) {
					int count = 0;
					if(wordCounts.containsKey(word)) {
						count = wordCounts.get(word);
					}
					wordCounts.put(word, count + 1);
				}
			}

			for(String word : this.wordCounts.keySet()) {
				out.collect(new Tuple3<String, String, Integer>(mail.f0, word, this.wordCounts.get(word)));
			}
		}
	}

	public static class TfIdfComputer implements JoinFunction<Tuple2<String, Integer>, Tuple3<String, String, Integer>, Tuple3<String, String, Double>> {

		private double docCount;

		public TfIdfComputer() {}

		public TfIdfComputer(long docCount) {
			this.docCount = (double)docCount;
		}

		@Override
		public Tuple3<String, String, Double> join(Tuple2<String, Integer> docFreq, Tuple3<String, String, Integer> termFreq) throws Exception {
			return new Tuple3<String, String, Double>(termFreq.f0, termFreq.f1, termFreq.f2 * (docCount / docFreq.f1));
		}
	}
}
