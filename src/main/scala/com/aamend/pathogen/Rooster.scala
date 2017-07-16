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
import org.apache.spark.HashPartitioner
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

class Rooster(config: Config) extends Serializable with LazyLogging {

  /**
    * @param events The initial time related events
    * @return the causal effects explained as a graph
    */
  def observe(events: RDD[Event]): Graph[Pathogen, Double] = {

    logger.info(s"Observing correlation across ${events.count()} time related events")
    val correlations = observeCorrelation(events)
    correlations.cache()
    val correlationCount = correlations.count()

    logger.info(s"Found $correlationCount possible correlations")

    val contagions = if (config.simulations > 1) {
      logger.info(s"Observing potential causes and effects")
      val causalities = observeCausation(events, correlations, config.simulations)
      normalize(events, causalities)
    } else {
      logger.warn("Correlation does not imply causation, proceed at your own risk")
      normalize(events, correlations)
    }

    val vertices = events.map(_.eventGroupId).distinct().map(l => {
      (l, Pathogen())
    })
    Graph.apply(vertices, contagions)

  }

  private def observeCorrelation(events: RDD[Event]): RDD[Edge[Double]] = {

    // Expand events for each tick between a start and an end date
    val eventTicks = events flatMap { event =>
      val ticks = DateUtils.getTicks(config.samplingRate, event.eventStart, event.eventEnd, config.inceptionWindow)
      ticks map (_ -> event)
    }

    // Group by timestamp and build time correlated events
    val vectors = eventTicks.groupByKey().values flatMap { it =>
      val events = it.toList.sortBy(_.eventStart)
      for (i <- 0 to events.length - 2; j <- i + 1 until events.length) yield {
        val source = events(i)
        val target = events(j)
        (source.eventGroupId, target.eventGroupId)
      }
    } map (_ -> 1) reduceByKey (_ + _)

    vectors map { case ((fromGroup, toGroup), obs) =>
      Edge(fromGroup, toGroup, obs.toDouble)
    }
  }

  private def observeCausation(
                                events: RDD[Event],
                                correlations: RDD[Edge[Double]],
                                simulations: Int
                              ): RDD[Edge[Double]] = {

    val minDate = events.map(_.eventStart).min()
    val maxDate = events.map(_.eventEnd).max()

    val simulationResults = {

      (1 to simulations) map { simulation =>

        // Generate random start date between minDate and maxDate, leaving the duration as-is
        val randomEvents = events map { event =>

          val duration = event.eventEnd - event.eventStart
          val localMaxDate = maxDate - duration

          // Because it is random, anytime we call a transformation, we might generate a new random value
          // We need to make sure a same event will always be generated the same FOR a same simulation
          val seed = s"""
                        |start=[${event.eventStart}]
                        |group=[${event.eventGroupId}]
                        |stop=[${event.eventEnd}]
                        |simulation=[$simulation]
             """.hashCode.toLong

          val randomStart = DateUtils.randomStartDate(minDate, localMaxDate, seed)
          val randomEnd = randomStart + duration
          Event(event.eventGroupId, randomStart, randomEnd)
        }

        val randomCorrelations = observeCorrelation(randomEvents)
        randomCorrelations.cache()
        val rcc = randomCorrelations.count()
        logger.info(s"Monte carlo $simulation/$simulations - $rcc correlations found")
        randomCorrelations

      }
    }

    logger.info("Normalizing causality score")
    val noiseHash = events.sparkContext.union(simulationResults) map { n =>
      (n.srcId + n.dstId, n.attr)
    } reduceByKey (_ + _) partitionBy new HashPartitioner(events.partitions.length)

    val signalHash = correlations map { s =>
      (s.srcId + s.dstId, s)
    } partitionBy new HashPartitioner(events.partitions.length)

    // Get actual causality score as a measure of Signal / AVG(Noise) ratio
    signalHash.leftOuterJoin(noiseHash).values map { case (signal, noise) =>
      val avgNoise = math.max(noise.getOrElse(0.0d) / simulations, 1.0d)
      Edge(signal.srcId, signal.dstId, signal.attr / avgNoise)
    }
  }

  private def normalize(events: RDD[Event], correlations: RDD[Edge[Double]]): RDD[Edge[Double]] = {
    val maxCausalityExplained = correlations.map(_.attr).max()
    correlations map { c =>
      Edge(c.srcId, c.dstId, c.attr / maxCausalityExplained)
    }
  }

}

object Rooster {
  implicit class RoosterProcessor(events: RDD[Event]) {
    def observe(config: Config): Graph[Pathogen, Double] = {
      new Rooster(config).observe(events)
    }
  }
}
