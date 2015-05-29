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

package com.dataArtisans.flinkTraining.exercises.dataSetScala.tfIdf

import java.util.StringTokenizer
import java.util.regex.Pattern

import com.dataArtisans.flinkTraining.dataSetPreparation.MBoxParser
import org.apache.flink.api.common.functions.{FlatMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.{HashMap, HashSet}

object MailTFIDF {

  val STOP_WORDS: Array[String] = Array (
    "the", "i", "a", "an", "at", "are", "am", "for", "and", "or", "is", "there", "it", "this",
    "that", "on", "was", "by", "of", "to", "in", "to", "message", "not", "be", "with", "you",
    "have", "as", "can")

   def main(args: Array[String]) {

     // set up the execution environment
     val env = ExecutionEnvironment.getExecutionEnvironment

     val mails = env.readCsvFile[(String, String)](
       "/users/fhueske/data/flinkdevlistparsed/",
       lineDelimiter = MBoxParser.MAIL_RECORD_DELIM,
       fieldDelimiter = MBoxParser.MAIL_FIELD_DELIM,
       includedFields = Array(0,4)
     )

     val docCnt = mails.count

     val tf = mails
       .flatMap(new TFComputer(STOP_WORDS))

     val idf = mails
       .flatMap(new UniqueWordExtractor(STOP_WORDS))
       .groupBy(0).reduceGroup { ws => ws.reduce( (l, r) => (l._1, l._2 + r._2 ) ) }

     tf.join(idf).where(1).equalTo(0) { (l, r) => (l._1, l._2, l._3 * (docCnt.toDouble / r._2) ) }
       .print


   }

  class TFComputer(stopWordsA: Array[String])
    extends FlatMapFunction[(String, String), (String, String, Int)] {

    val stopWords: HashSet[String] = new HashSet[String]
    val wordCounts: HashMap[String, Int] = new HashMap[String, Int]
    val wordPattern: Pattern = Pattern.compile("(\\p{Alpha})+")

    for(sw <- stopWordsA) {
      this.stopWords.add(sw)
    }

    override def flatMap(t: (String, String), out: Collector[(String, String, Int)]): Unit = {
      wordCounts.clear

      val tokens = new StringTokenizer(t._2)
      while (tokens.hasMoreTokens) {
        val word = tokens.nextToken.toLowerCase
        if (!stopWords.contains(word) && wordPattern.matcher(word).matches) {

          val cnt = wordCounts.getOrElse(word, 0)
          wordCounts.put(word, cnt+1)
        }
      }
      for (wc <- wordCounts.iterator) {
        out.collect( (t._1, wc._1, wc._2) )
      }
    }
  }

  class UniqueWordExtractor(stopWordsA: Array[String])
    extends FlatMapFunction[(String, String), (String, Int)] {

    val stopWords: HashSet[String] = new HashSet[String]
    val uniqueWords: HashSet[String] = new HashSet[String]
    val wordPattern: Pattern = Pattern.compile("(\\p{Alpha})+")

    for(sw <- stopWordsA) {
      this.stopWords.add(sw)
    }

    override def flatMap(t: (String, String), out: Collector[(String, Int)]): Unit = {

      uniqueWords.clear()

      val tokens = new StringTokenizer(t._2)
      while(tokens.hasMoreTokens) {
        val word = tokens.nextToken.toLowerCase
        if (!stopWords.contains(word) && wordPattern.matcher(word).matches) {
          uniqueWords.add(word)
        }
      }

      for(w <- uniqueWords) {
        out.collect( (w, 1) )
      }
    }
  }

 }


