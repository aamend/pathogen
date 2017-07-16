/*
 * Copyright 2017 Antoine Amend
 *
 * Pathogen is licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aamend.pathogen

import com.aamend.pathogen.Sun.VertexData
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.graphx._

import scala.util.Try

class Sun(config: Config) extends Serializable with LazyLogging {

  /**
    * @param causalGraph The graph of observed (and normalized) correlations
    * @return The Contagion graph where vertices contain both a sensitivity and aggressiveness scores
    */
  def explain(causalGraph: Graph[Pathogen, Double]): Graph[Pathogen, Double] = {

    // ---------------
    // SENSITIVITY
    // ---------------

    val vertexSensitivity = propagateCausality(causalGraph, config.tolerance, config.maxIterations, config.forgetfulness)
    val totalSensitivity = vertexSensitivity.values.sum()
    logger.info(s"Total sensitivity is ${"%.2f".format(totalSensitivity)}")

    // ---------------
    // AGGRESSIVENESS
    // ---------------

    val vertexAggressiveness = propagateCausality(causalGraph.reverse, config.tolerance, config.maxIterations, config.forgetfulness)
    val totalAggressiveness = vertexAggressiveness.values.sum()
    logger.info(s"Total aggressiveness is ${"%.2f".format(totalAggressiveness)}")

    // ---------------
    // PATHOGEN
    // ---------------

    causalGraph.outerJoinVertices(vertexAggressiveness.fullOuterJoin(vertexAggressiveness))({ case (vId, _, opt) =>
      Pathogen(
        Try {
          opt.get._2.get
        }.getOrElse(0.0d) / totalAggressiveness,
        Try {
          opt.get._1.get
        }.getOrElse(0.0d) / totalSensitivity
      )
    })

  }

  private def propagateCausality(
                                  causalGraph: Graph[Pathogen, Double],
                                  tolerance: Float,
                                  maxIterations: Int,
                                  decayFactor: Float
                                ): VertexRDD[Double] = {

    def sendInitMessage(e: EdgeContext[Pathogen, Double, Map[Long, Double]]) = {
      e.sendToSrc(Map(e.dstId -> e.attr))
    }

    def mergeInitMessage(v1: Map[Long, Double], v2: Map[Long, Double]) = {
      v1 ++ v2
    }

    val graph = causalGraph.outerJoinVertices(
      causalGraph.aggregateMessages(sendInitMessage, mergeInitMessage)
    ) { case (_, _, outCausality) =>
      outCausality.getOrElse(Map())
    } mapVertices { case (_, outCausality) =>
      // We start with an initial weight of 0
      // We start with a contribution received of 0
      // This pathogen has never been active yet
      val totalDegrees = outCausality.size
      val totalCausality = outCausality.values.sum
      VertexData(0.0d, 0.0d, totalDegrees, totalCausality, 0)
    }

    graph.cache()
    val vertices = graph.vertices.count()
    val edges = graph.edges.count()
    logger.info(s"Starting explaining causality on $vertices hosts and $edges connections")

    // Execute Pregel to source to destination nodes
    val propagated = graph.pregel(
      -1.0d,
      maxIterations,
      EdgeDirection.Out
    )(
      pregelUpdate,
      pregelSend,
      pregelMerge
    ) mapVertices { case (_, vData) =>
      // Do not forget the left over from vertex tuple
      (vData.receivedWeight + vData.internalWeight, vData.activated)
    }

    propagated.cache()
    val averageSteps = "%.2f".format(propagated.vertices.map(_._2._2).sum() / vertices.toDouble)
    val maxSteps = propagated.vertices.values.values.max()
    if (maxSteps == maxIterations) {
      val nonConverged = propagated.vertices.filter(_._2._2 == maxSteps).count()
      logger.warn(s"$nonConverged/$vertices nodes did not converge after $maxIterations iterations")
    } else {
      logger.info(s"Pathogen converged after $averageSteps steps in average, max is $maxSteps")
    }

    propagated.vertices.mapValues(_._1)

  }

  private def pregelUpdate(vId: VertexId, vData: VertexData, msg: Double): VertexData = {
    if (msg < 0) {
      // Initial message, this is the very first event triggered
      vData
    } else {
      // We update the total score of the vertex attribute
      // We store the new transitive contribution (will be sent at S+1)
      // We increment the number of times this pathogen became active
      VertexData(
        msg,
        vData.internalWeight + vData.receivedWeight,
        vData.outDegrees,
        vData.outCausality,
        vData.activated + 1
      )
    }
  }

  private def pregelMerge(msg1: Double, msg2: Double): Double = msg1 + msg2

  private def pregelSend(triplet: EdgeTriplet[VertexData, Double]): Iterator[(VertexId, Double)] = {
    val src = triplet.srcAttr
    if (src.activated == 0) {
      // On a very first step: We send the entire source causality (stored as edge weight) to the destination node
      Iterator((triplet.dstId, triplet.attr))
    } else {
      // On further steps: A node becomes inactive if
      // - The new contribution brings less than a X% increase
      // - It does not have any destination node
      val increase = src.receivedWeight / src.internalWeight
      if (increase > config.tolerance) {
        // We send a contribution of the transitive causality
        // This contribution is shared across destination nodes according to their pre-defined causality scores
        // This contribution is decayed by a given factor
        val toPropagate = src.receivedWeight * (1.0f - config.forgetfulness)
        val relativeCausality = triplet.attr / src.outCausality
        val contribution = toPropagate * relativeCausality
        Iterator((triplet.dstId, contribution))
      } else {
        Iterator.empty
      }
    }
  }

}

object Sun {

  case class VertexData(
                         receivedWeight: Double,
                         internalWeight: Double,
                         outDegrees: Int,
                         outCausality: Double,
                         activated: Int
                       )

  implicit class SunProcessor(causalGraph: Graph[Pathogen, Double]) {
    def explain(config: Config): Graph[Pathogen, Double] = {
      new Sun(config).explain(causalGraph)
    }
  }

}