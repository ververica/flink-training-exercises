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

import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool

import com.dataartisans.flinktraining.dataset_preparation.MBoxParser

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

  // stop words
  private val STOP_WORDS = Set (
    "the", "i", "a", "an", "at", "are", "am", "for", "and", "or", "is", "there", "it", "this",
    "that", "on", "was", "by", "of", "to", "in", "to", "message", "not", "be", "with", "you",
    "have", "as", "can")

  // word pattern regular expression
  private val WORD_PATTERN = "(\\p{Alpha})+".r

  def main(args: Array[String]) {

    // parse parameters
    val params = ParameterTool.fromArgs(args)
    val input = params.getRequired("input")

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // function returns true if string is not a stop word and matches the word pattern regex
    val isWord = (s : String) => !STOP_WORDS.contains(s) && WORD_PATTERN.findFirstIn(s).isDefined

    // read messageId and body field of the input data
    val mails = env.readCsvFile[(String, String)](
      input,
      lineDelimiter = MBoxParser.MAIL_RECORD_DELIM,
      fieldDelimiter = MBoxParser.MAIL_FIELD_DELIM,
      includedFields = Array(0,4)
    ) map (m => (m._1, m._2.toLowerCase.split("\\W+")  // convert message to lower case and split on word boundary
      .filter(s => isWord(s)))) // retain only those strings that are words

    // count mails in data set
    val cnt = mails.count

    // mails is a DataSet[String, Array[String]]
    // for each mail, use Array.groupBy(w => w) to create a Map[(String, Array[String])]
    //    where the key is a string and the value an Array containing all strings.
    //    Then use map on the entries of the Map to pick out:
    //       e._1: the key and
    //       e._2.size: the size of each Array[String] (number of times a string occurred)
    val tfs = mails flatMap (m => m._2.groupBy(w => w).map(e => (m._1, e._1, e._2.length)))

    // Extract unique words by creating a set out of the Array[String] of each mail
    // create a (m, 1) tuple for each string
    val dfs = (mails flatMap (m => m._2.toSet)) // Extract unique words of each mail converting Array[String] to a Set[String]
      .map (m => (m, 1))  // Create the (string, 1) initial record for each unique string
      .groupBy(0).reduce ((l, r) => (l._1, l._2 + r._2)) // count the number of mails for each word

    // compute TF-IDF score from TF, DF, and total number of mails
    val tfidfs = tfs.join(dfs).where(1).equalTo(0) { (l, r) => (l._1, l._2, l._3 * (cnt.toDouble / r._2) ) }

    tfidfs.print
  }
 }

