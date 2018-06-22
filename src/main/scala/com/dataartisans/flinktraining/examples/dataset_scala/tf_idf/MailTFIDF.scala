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

package com.dataartisans.flinktraining.examples.dataset_scala.tf_idf

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
    val isRelevantWord: (String => Boolean) = {
      s => !STOP_WORDS.contains(s) && WORD_PATTERN.unapplySeq(s).isDefined
    }

    // read messageId and body field of the input data
    val mails : DataSet[(String,Array[String])] = env
      .readCsvFile[(String, String)](
        input,
        lineDelimiter = MBoxParser.MAIL_RECORD_DELIM,
        fieldDelimiter = MBoxParser.MAIL_FIELD_DELIM,
        includedFields = Array(0,4)
      )
      // convert message to lower case and split on word boundary
      .map (m => (m._1, m._2.toLowerCase.split("\\s")
      // retain only relevant words
      .filter(s => isRelevantWord(s))))

    // count mails in data set
    val cnt = mails.count

    // For each mail, compute the frequency of words in the mail
    val tfs : DataSet[(String, String, Int)] = mails
      .flatMap { m =>
        m._2
          // For each word in a mail, create a Map where the key is the word and the value is an
          // Array containing all occurrences of that word.
          .groupBy(w => w)
          // for each entry in the Map, create a tuple of messageId, the word in the mail,
          // and the number of occurrences of the word in the mail.
          .map(e => (m._1, e._1, e._2.length))
      }

    // For each unique word, compute the number of mails it is contained in
    val dfs : DataSet[(String, Int)] = mails
      // Extract unique words of each mail converting Array[String] to a Set[String]
      .flatMap (m => m._2.toSet)
      // Emit (word, 1) for each unique word in a mail
      .map (m => (m, 1))
      // group by words
      .groupBy(0)
      // count the number of mails for each word by summing the ones.
      .sum(1)

    // compute TF-IDF score from TF, DF, and total number of mails
    val tfidfs : DataSet[(String, String, Double)] = tfs.join(dfs).where(1).equalTo(0) {
      (l, r) => (l._1, l._2, l._3 * (cnt.toDouble / r._2) )
    }

    tfidfs.print
  }
}

