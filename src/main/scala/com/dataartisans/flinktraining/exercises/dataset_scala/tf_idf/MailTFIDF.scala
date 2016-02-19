package org.apache.flink

/**
 * Copyright 2015 data Artisans GmbH
 *
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

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.api.java.utils.ParameterTool

/**
 * Scala reference implementation for the "TF-IDF" exercise of the Flink training.
 * The task of the exercise is to compute the TF-IDF score for words in mails of the
 * Apache Flink developer mailing list archive.
 *
 * Required parameters:
 *   --input path-to-input-directory [--odir path-to-output-directory (defaults to /tmp)]
 *
 */

object MailTFIDF {
  def main(args: Array[String]) {
    val params = ParameterTool.fromArgs(args)
    val input = params.getRequired("input")
    val odir = params.get("odir", "/tmp")

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    val stopWords = Set(
      "the", "i", "a", "an", "at", "are", "am", "for", "and", "or", "is", "there", "it", "this",
      "that", "on", "was", "by", "of", "to", "in", "to", "not", "be", "with", "you",
      "have", "as", "can")

    val wordPattern = "(\\p{Alpha})+".r

    val isWord = (s : String) => (!stopWords.contains(s) && wordPattern.findFirstIn(s).isDefined)

    val mails = env.readCsvFile[(String, String)](
      input,
      lineDelimiter = "##//##",
      fieldDelimiter = "#|#",
      includedFields = Array(0,4)
    // ) map (m => (m._1, m._2.toLowerCase.split("\\W+").collect{case s if isWord(s) => s})) first (1000)
    ) map (m => (m._1, m._2.toLowerCase.split("\\W+").filter(s => (isWord(s))))) first (1000) // TODO : remove first (1000) i was running on a vm

    val cnt = mails.count
    // val tfs = mails flatMap (m => m._2.groupBy(w => w).mapValues(_.size).map(e => (m._1, e._1, e._2)))
    val tfs = mails flatMap (m => m._2.groupBy(w => w).map(e => (m._1, e._1, e._2.size)))
    val dfs = (mails flatMap (m => m._2.toSet)) map (m => (m, 1)) groupBy(0) reduce ((l, r) => (l._1, l._2 + r._2))
    val tfidfs = tfs.join(dfs).where(1).equalTo(0) { (l, r) => (l._1, l._2, l._3 * (cnt.toDouble / r._2) ) }

    // mails.map(m => (m._1, m._2.mkString(" "))).writeAsText(s"file://${odir}/MAILS.csv", WriteMode.OVERWRITE)
    tfs.writeAsText(s"file://${odir}/TFs.csv", WriteMode.OVERWRITE)
    dfs.writeAsText(s"file://${odir}/DFs.csv", WriteMode.OVERWRITE)
    tfidfs.writeAsText(s"file://${odir}/TFIDFs.csv", WriteMode.OVERWRITE)

    // execute program
    env.execute("Flink Scala API Skeleton")
  }
}
