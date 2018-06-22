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

package com.dataartisans.flinktraining.examples.dataset_scala.reply_graph

import com.dataartisans.flinktraining.dataset_preparation.MBoxParser
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._

/**
 * Scala reference implementation for the "Reply Graph" exercise of the Flink training.
 * The task of the exercise is to enumerate the reply connection between two email addresses in
 * Flink's developer mailing list and count the number of connections between two email addresses.
 *
 * Required parameters:
 *   --input path-to-input-directory
 */
object ReplyGraph {
  def main(args: Array[String]) {

    // parse parameters
    val params = ParameterTool.fromArgs(args)
    val input = params.getRequired("input")

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // read messageId, sender, and reply-to fields from input data set
    val mails = env.readCsvFile[(String, String, String)](
      input,
      lineDelimiter = MBoxParser.MAIL_RECORD_DELIM,
      fieldDelimiter = MBoxParser.MAIL_FIELD_DELIM,
      includedFields = Array(0,2,5)
    )

    // extract email addresses and filter out mails sent from bots
    val addressMails = mails
      // extract email addresses
      .map { m => ( m._1,
                    m._2.substring(m._2.lastIndexOf("<") + 1, m._2.length - 1),
                    m._3 ) }
      // filter out mails
      .filter { m => !(m._2.equals("git@git.apache.org") || m._2.equals("jira@apache.org")) }

    // compute reply connections by joining on messageId and reply-to
    val replyConnections = addressMails
      .join(addressMails).where(2).equalTo(0) { (l,r) => (l._2, r._2) }

    // count connections for each pair of addresses
    replyConnections
      .groupBy(0,1).reduceGroup( cs => cs.foldLeft(("","",0))( (l,r) => (r._1, r._2, l._3+1) ) )
      .print

  }

}
