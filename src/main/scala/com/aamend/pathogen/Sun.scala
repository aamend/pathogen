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

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.graphx._

import scala.util.Try

/**
  * @param maxIterations The maximum number of iterations in Pregel
  * @param tolerance     The absolute zero when convergence is reached
  * @param teleport      Whether or not to allow teleport of causes and effects
  * @param decay         Whether a decay factor must be applied at each transition
  */
class Sun(
           maxIterations: Int = 100,
           tolerance: Float = 0.001f,
           teleport: Float = 0.0f,
           decay: Float = 0.0f
         ) extends Serializable with LazyLogging {

  /**
    * @param causalGraph The graph of observed (and normalized) correlations
    * @return The Contagion graph where vertices contain both a sensitivity and aggressivness scores
    */
  def explainRooster(causalGraph: Graph[String, Double]): Graph[Pathogen, Double] = {

    // ---------------
    // SENSITIVITY
    // ---------------

    val vertexSensitivity = propagateCausality(causalGraph, tolerance, maxIterations, decay)
    val totalSensitivity = vertexSensitivity.values.sum()
    logger.info(s"Total sensitivity is ${"%.2f".format(totalSensitivity)}")

    // ---------------
    // AGGRESSIVENESS
    // ---------------

    val vertexAggressiveness = propagateCausality(causalGraph.reverse, tolerance, maxIterations, decay)
    val totalAggressiveness = vertexAggressiveness.values.sum()
    logger.info(s"Total aggressiveness is ${"%.2f".format(totalAggressiveness)}")

    // ---------------
    // PATHOGEN
    // ---------------

    causalGraph.outerJoinVertices(vertexAggressiveness.fullOuterJoin(vertexAggressiveness))({ case (vId, _, opt) =>
      Pathogen(
        vId,
        Try {
          opt.get._2.get
        }.getOrElse(0.0d) / totalAggressiveness,
        Try {
          opt.get._1.get
        }.getOrElse(0.0d) / totalSensitivity
      )
    })

  }

  private def propagateCausality(causalGraph: Graph[String, Double], tolerance: Float, maxIterations: Int, decayFactor: Float): VertexRDD[Double] = {

    // Find the number of outDegree for each vertex
    val graph = causalGraph.outerJoinVertices(causalGraph.outDegrees) { case (_, _, outDegrees) =>
      outDegrees.getOrElse(0)
    } mapVertices { case (_, outDegrees) =>
      // We start with an initial weight of 0
      // We start with a contribution received of 0
      // This pathogen has never been active yet
      VertexData(0.0d, 0.0d, outDegrees, 0)
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
      (vData.lastReceived + vData.internalWeight, vData.active)
    }

    // Get some stats about convergence
    propagated.cache()
    val averageSteps = "%.2f".format(propagated.vertices.map(_._2._2).sum() / vertices.toDouble)
    val maxSteps = propagated.vertices.map(_._2._2).max()
    if (maxSteps == maxIterations) {
      val nonConverged = propagated.vertices.filter(_._2._2 == maxSteps).count()
      logger.warn(s"$nonConverged/$vertices nodes did not converge after $maxIterations iterations")
    } else {
      logger.info(s"Pandemic converged after $averageSteps steps in average, max is $maxSteps")
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
        vData.internalWeight + vData.lastReceived,
        vData.outDegrees,
        vData.active + 1
      )
    }
  }

  private def pregelMerge(msg1: Double, msg2: Double): Double = msg1 + msg2

  private def pregelSend(triplet: EdgeTriplet[VertexData, Double]): Iterator[(VertexId, Double)] = {
    val src = triplet.srcAttr
    if (src.active == 0) {
      // On a very first step: We send the entire source causality (stored as edge weight) to the destination node
      Iterator((triplet.dstId, triplet.attr))
    } else {
      // On further steps: A node becomes inactive if
      // - The new contribution brings less than a X% increase
      // - It does not have any destination node
      val increase = src.lastReceived / src.internalWeight
      if (increase > tolerance && src.outDegrees > 0) {
        // We send a contribution of the transitive causality
        // This contribution is evenly shared across destination nodes
        // This contribution is decayed by a given factor
        // TODO: This contribution may be shared across non connected nodes (teleport)
        val contribution = src.lastReceived * (1.0f - decay) / src.outDegrees
        Iterator((triplet.dstId, contribution))
      } else {
        Iterator.empty
      }
    }
  }

}