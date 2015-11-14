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

package com.dataArtisans.flinkTraining.exercises.gellyScala

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala._
import org.apache.flink.graph.scala._
import org.apache.flink.graph.scala.utils.Tuple3ToEdgeMap
import org.apache.flink.graph.{EdgeDirection, EdgeJoinFunction, ReduceEdgesFunction}

/**
  *
  * The edges input file is expected to contain one edge per line, with String IDs and double
  * values in the following format:"<sourceVertexID>\t<targetVertexID>\t<edgeValue>".
  *
  * This class is used to create a graph from the input data and then to run a PageRankAlgorithm
  * (present in Flink-gelly graph-library)over it. The algorithm used is a simplified implementation
  * of the actual algorithm; its limitation is that all the pages need to have at least one incoming
  * and one outgoing link for correct results. The vertex-centric algorithm takes as input parameters
  * dampening factor and number of iterations.
  *
  */

object PageRankWithEdgeWeights {

  private val DAMPENING_FACTOR: Double = 0.85

  private var edgesInputPath: String = null
  private var outputPath: String = null
  private var maxIterations: Int = 0

  @throws(classOf[Exception])
  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    //read the Edge DataSet from the input file
    val links = env.readCsvFile[(String, String, Double)](edgesInputPath,
      fieldDelimiter = "\t", lineDelimiter = "\n").map(new Tuple3ToEdgeMap[String, Double]())

    //create a Graph with vertex values initialized to 1.0
    val network = Graph.fromDataSet(links, new MapFunction[String, Double]() {
      def map(value: String): Double = { 1.0 }
    }, env)

    //for each vertex calculate the total weight of its outgoing edges
    val sumEdgeWeights = network.reduceOnEdges(new SumWeight(), EdgeDirection.OUT)

    // assign the transition probabilities as edge weights:
    // divide edge weight by the total weight of outgoing edges for that source
    val networkWithWeights = network.joinWithEdgesOnSource(sumEdgeWeights,
      new EdgeJoinFunction[Double, Double]() {
        def edgeJoin(d1: Double, d2: Double) = {
          d1 / d2
        }
      })

    //Now run the Page Rank algorithm over the weighted graph
//    val pageRanks = networkWithWeights.run[String](new PageRank[String, Double, Double, String](DAMPENING_FACTOR, maxIterations))

//    pageRanks.writeAsCsv(outputPath, fieldDelimiter = "\t", lineDelimiter = "\n")

    env.execute("Run PageRank with Edge Weights")

  }

  private def parseParameters(args: Array[String]): Boolean = {
    if(args.length > 0) {
      if(args.length != 3) {
        System.err.println("Usage PageRankWithEdgeWeights <edge path> <output path> <num iterations>")
        false
      }
      edgesInputPath = args(0)
      outputPath = args(1)
      maxIterations = 2
    }
    true
  }
}

//function to calculate the total weight of outgoing edges from a node
class SumWeight extends ReduceEdgesFunction[Double] {
  override def reduceEdges(firstEdgeValue: Double, secondEdgeValue: Double): Double = firstEdgeValue
}

