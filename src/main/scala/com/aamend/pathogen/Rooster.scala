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

import com.aamend.pathogen.DateUtils.Frequency
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.HashPartitioner
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  * @param samplingRate      Frequency used to detect time correlation (Hour, Day, Month, etc.)
  * @param inceptionWindow   Whether an inception time must be deduced from first observation tick
  * @param groupBy           The metadata attribute to use as a grouping parameter
  * @param simulations       The number of simulations to run
  */
class Rooster(
               groupBy: String,
               samplingRate: Frequency.Value = Frequency.DAY,
               inceptionWindow: Int = 0,
               simulations: Int = 100
             ) extends Serializable with LazyLogging {

  /**
    * @param events The initial time related events
    * @return the causal effects explained as a graph
    */
  def observeSun(events: RDD[Event]): Graph[VertexId, Double] = {

    logger.info(s"Observing correlation across ${events.count()} time related events")
    val correlations = observeCorrelation(events)
    correlations.cache()
    val correlationCount = correlations.count()

    logger.info(s"Found $correlationCount possible correlations")

    val contagions = if (simulations > 1) {
      logger.info(s"Observing potential causes and effects")
      val causalities = observeCausation(events, correlations, simulations)
      normalize(events, causalities)
    } else {
      logger.warn("Correlation does not imply causation, proceed at your own risk")
      normalize(events, correlations)
    }

    // Each edge contains the initial causality contribution
    val edges = contagions mapPartitions { it =>
      it map { contagion =>
        Edge(contagion.source, contagion.target, contagion.causality)
      }
    }

    Graph.fromEdges(edges, 0L)

  }

  private def observeCorrelation(events: RDD[Event]): RDD[Correlation] = {

    // Expand events for each tick between a start and an end date
    val eventTicks = events flatMap { event =>
      val ticks = DateUtils.getTicks(samplingRate, event.start, event.end, inceptionWindow)
      ticks map (_ -> event)
    }

    // Group by timestamp and build time correlated events
    val vectors = eventTicks.groupByKey().values flatMap { it =>
      val events = it.toList.sortBy(_.start)
      for (i <- 0 to events.length - 2; j <- i + 1 until events.length) yield {
        val source = events(i)
        val target = events(j)
        (source.id, target.id)
      }
    } map (_ -> 1) reduceByKey (_ + _)

    vectors map { case ((fromGroup, toGroup), obs) =>
      Correlation(fromGroup, toGroup, obs.toDouble)
    }
  }

  private def observeCausation(events: RDD[Event], correlations: RDD[Correlation], simulations: Int): RDD[Correlation] = {

    val minDate = events.map(_.start).min()
    val maxDate = events.map(_.end).max()

    val simulationResults = {

      (1 to simulations) map { simulation =>

        // Generate random start date between minDate and maxDate, leaving the duration as-is
        val randomEvents = events map { event =>
          val duration = event.end - event.start
          val localMaxDate = maxDate - duration
          val randomStart = DateUtils.randomStartDate(minDate, localMaxDate)
          val randomEnd = randomStart + duration
          Event(event.id, randomStart, randomEnd)
        }

        // Because it is random, anytime we call a transformation, we might generate a new random value
        // We make sure this is properly cached - at the expense of overall performance
        randomEvents.persist(StorageLevel.DISK_ONLY_2)
        randomEvents.count()

        val randomCorrelations = observeCorrelation(randomEvents)
        randomCorrelations.cache()

        // Materialize cache
        val rcc = randomCorrelations.count()
        logger.info(s"Monte carlo $simulation/$simulations - $rcc correlations found")

        randomEvents.unpersist(blocking = true)
        randomCorrelations

      }
    }

    logger.info("Normalizing causality score")
    val noiseHash = events.sparkContext.union(simulationResults) map { n =>
      (n.source + n.target, n.causality)
    } reduceByKey (_ + _) partitionBy new HashPartitioner(events.partitions.length)

    val signalHash = correlations map { s =>
      (s.source + s.target, s)
    } partitionBy new HashPartitioner(events.partitions.length)

    // Get actual causality score as a measure of Signal / AVG(Noise) ratio
    signalHash.leftOuterJoin(noiseHash).values map { case (signal, noise) =>
      val avgNoise = math.max(noise.getOrElse(0.0d) / simulations, 1.0d)
      Correlation(signal.source, signal.target, signal.causality / avgNoise)
    }
  }

  private def normalize(events: RDD[Event], correlations: RDD[Correlation]): RDD[Correlation] = {
    val maxCausalityExplained = correlations.map(_.causality).max()
    correlations map { c =>
      Correlation(c.source, c.target, c.causality / maxCausalityExplained)
    }
  }

}
