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

package com.dataartisans.flinktraining.exercises.dataset_scala.tf_idf

import java.util.StringTokenizer
import java.util.regex.Pattern
import com.dataartisans.flinktraining.dataset_preparation.MBoxParser
import org.apache.flink.api.common.functions.{FlatMapFunction}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.{HashMap, HashSet}

/**
 * Scala reference implementation for the "TF-IDF" exercise of the Flink training.
 * The task of the exercise is to compute the TF-IDF score for words in mails of the
 * Apache Flink developer mailing list archive.
 *
 * Required parameters:
 *   --input path-to-input-directory
 *
 */
object MailTFIDF {

  val STOP_WORDS: Array[String] = Array (
    "the", "i", "a", "an", "at", "are", "am", "for", "and", "or", "is", "there", "it", "this",
    "that", "on", "was", "by", "of", "to", "in", "to", "message", "not", "be", "with", "you",
    "have", "as", "can")

   def main(args: Array[String]) {

     // parse paramters
     val params = ParameterTool.fromArgs(args);
     val input = params.getRequired("input");

     // set up the execution environment
     val env = ExecutionEnvironment.getExecutionEnvironment

     // read messageId and body field of the input data
     val mails = env.readCsvFile[(String, String)](
       "/users/fhueske/data/flinkdevlistparsed/",
       lineDelimiter = MBoxParser.MAIL_RECORD_DELIM,
       fieldDelimiter = MBoxParser.MAIL_FIELD_DELIM,
       includedFields = Array(0,4)
     )

     // count mails in data set
     val mailCnt = mails.count

     // compute term-frequency (TF)
     val tf = mails
       .flatMap(new TFComputer(STOP_WORDS))

     // compute document frequency (number of mails that contain a word at least once)
     val df = mails
       // extract unique words from mails
       .flatMap(new UniqueWordExtractor(STOP_WORDS))
       // count number of mails for each word
       .groupBy(0).reduce { (l,r) => (l._1, l._2 + r._2) }

     // compute TF-IDF score from TF, DF, and total number of mails
     val tfidf = tf.join(df).where(1).equalTo(0)
                      { (l, r) => (l._1, l._2, l._3 * (mailCnt.toDouble / r._2) ) }

     // print the result
     tfidf
       .print

   }

  /**
   * Computes the frequency of each word in a mail.
   * Words consist only of alphabetical characters. Frequent words (stop words) are filtered out.
   *
   * @param stopWordsA  Array of words that are filtered out.
   */
  class TFComputer(stopWordsA: Array[String])
    extends FlatMapFunction[(String, String), (String, String, Int)] {

    val stopWords: HashSet[String] = new HashSet[String]
    val wordCounts: HashMap[String, Int] = new HashMap[String, Int]
    // initialize word pattern match for sequences of alphabetical characters
    val wordPattern: Pattern = Pattern.compile("(\\p{Alpha})+")

    // initialize set of stop words
    for(sw <- stopWordsA) {
      this.stopWords.add(sw)
    }

    override def flatMap(t: (String, String), out: Collector[(String, String, Int)]): Unit = {
      // clear word counts
      wordCounts.clear

      // split mail along whitespaces
      val tokens = new StringTokenizer(t._2)
      // for each word candidate
      while (tokens.hasMoreTokens) {
        // normalize word to lower case
        val word = tokens.nextToken.toLowerCase
        if (!stopWords.contains(word) && wordPattern.matcher(word).matches) {
          // word candidate is not a stop word and matches the word pattern
          // increase word count
          val cnt = wordCounts.getOrElse(word, 0)
          wordCounts.put(word, cnt+1)
        }
      }
      // emit all word counts per document and word
      for (wc <- wordCounts.iterator) {
        out.collect( (t._1, wc._1, wc._2) )
      }
    }
  }

  /**
   * Extracts the unique words in a mail.
   * Words consist only of alphabetical characters. Frequent words (stop words) are filtered out.
   *
   * @param stopWordsA  Array of words that are filtered out.
   */
  class UniqueWordExtractor(stopWordsA: Array[String])
    extends FlatMapFunction[(String, String), (String, Int) ] {

    val stopWords: HashSet[String] = new HashSet[String]
    val uniqueWords: HashSet[String] = new HashSet[String]
    // initalize pattern to match words
    val wordPattern: Pattern = Pattern.compile("(\\p{Alpha})+")

    // initialize set of stop words
    for(sw <- stopWordsA) {
      this.stopWords.add(sw)
    }

    override def flatMap(t: (String, String), out: Collector[(String, Int)]): Unit = {
      // clear unique words
      uniqueWords.clear()

      // split mail along whitespaces
      val tokens = new StringTokenizer(t._2)
      // for each word candidate
      while(tokens.hasMoreTokens) {
        // normalize word to lower case
        val word = tokens.nextToken.toLowerCase
        if (!stopWords.contains(word) && wordPattern.matcher(word).matches) {
          // word candiate is not a stop word and matches the word pattern
          uniqueWords.add(word)
        }
      }

      // emit all words that occurred at least once
      for(w <- uniqueWords) {
        out.collect( (w, 1) )
      }
    }
  }

 }


