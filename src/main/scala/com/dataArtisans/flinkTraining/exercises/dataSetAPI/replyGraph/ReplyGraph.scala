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

package com.dataArtisans.flinkTraining.exercises.dataSetAPI.replyGraph

import com.dataArtisans.flinkTraining.dataSetPreparation.MBoxParser
import org.apache.flink.api.scala._

object ReplyGraph {
  def main(args: Array[String]) {

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    val mails = env.readCsvFile[(String, String, String)](
      "/users/fhueske/data/flinkdevlistparsed/",
      lineDelimiter = MBoxParser.MAIL_RECORD_DELIM,
      fieldDelimiter = MBoxParser.MAIL_FIELD_DELIM,
      includedFields = Array(0,2,5)
    )

    val addressMails = mails
      .map { m => ( m._1,
                    m._2.substring(m._2.lastIndexOf("<") + 1, m._2.length - 1),
                    m._3 ) }
      .filter { m => !(m._2.equals("git@git.apache.org") || m._2.equals("jira@apache.org")) }

    val replyConnections = addressMails
      .join(addressMails).where(2).equalTo(0) { (l,r) => (l._2, r._2) }

    replyConnections
      .map { c => (c._1, c._2, 1) }
      .groupBy(0,1).sum(2)
      .print

  }

}
