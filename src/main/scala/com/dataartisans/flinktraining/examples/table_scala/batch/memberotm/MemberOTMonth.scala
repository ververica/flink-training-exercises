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

package com.dataartisans.flinktraining.examples.table_scala.batch.memberotm

import com.dataartisans.flinktraining.dataset_preparation.MBoxParser
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * Scala reference implementation for the "Member of the Month" exercise of the Flink training.
 * The task of the exercise is to identify for each month the email address that sent the most
 * emails to the Flink developer mailing list.
 *
 * Required parameters:
 * --input path-to-input-directory
 *
 */
object MemberOTMonth {
  def main(args: Array[String]) {

    // parse parameters
    val params = ParameterTool.fromArgs(args)
    val input = params.getRequired("input")

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    // read the "time" and "sender" fields of the input data set as Strings
    val mails = env.readCsvFile[(String, String)](
      input,
      lineDelimiter = MBoxParser.MAIL_RECORD_DELIM,
      fieldDelimiter = MBoxParser.MAIL_FIELD_DELIM,
      includedFields = Array(1,2)
    )

    val mailsPerSenderMonth = mails
      .map { m => (
                    // extract month from time string
                    m._1.substring(0, 7),
                    // extract email address from sender
                    m._2.substring(m._2.lastIndexOf("<") + 1, m._2.length - 1) ) }
      // convert to table
      .toTable(tEnv, 'month, 'sender)
      // filter out bot mails
      .filter(('sender !== "jira@apache.org") &&
              ('sender !== "git@git.apache.org") &&
              ('sender !== "no-reply@apache.org"))
      // count emails per month and sender
      .groupBy('month, 'sender).select('month, 'sender, 'month.count as 'cnt)

    val membersOTMonth = mailsPerSenderMonth
      // find max number of mails send in each month
      .groupBy('month).select('month as 'm, 'cnt.max as 'max)
      // find email address that sent the most emails per month
      .join(mailsPerSenderMonth).where('m === 'month && 'max === 'cnt).select('month, 'sender)

    // print out result
    membersOTMonth.toDataSet[Row].print()
  }

}