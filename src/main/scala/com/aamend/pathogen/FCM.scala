/*
 * Copyright (C) 2017 Antoine Amend
 *
 * This file is part of Pathogen.
 *
 * Pathogen is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Pathogen is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Pathogen.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.aamend.pathogen

import com.aamend.pathogen.DateUtils.Frequency
import com.aamend.pathogen.Rooster._
import com.aamend.pathogen.Sun._
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD

object FCM {

  /**
    * Build a Fuzzy Cognitive Map from time related events
    *
    * @param events the RDD of time events
    * @param config the FCM config
    * @return the graph of connection together with their associated metrics and strengths
    */
  def build(events: RDD[Event], config: Config = Config()): Graph[Pathogen, Double] = {
    events observeSun(
      config.samplingRate,
      config.inceptionWindow,
      config.simulations
    ) explainRooster(
      config.maxIterations,
      config.tolerance,
      config.forgetfulness
    )
  }

  implicit class FCMProcessor(events: RDD[Event]) {
    def fcm(config: Config = Config()): Graph[Pathogen, Double] = {
      FCM.build(events, config)
    }
  }

  case class Config(
                     samplingRate: Frequency.Value = Frequency.DAY,
                     inceptionWindow: Int = 0,
                     simulations: Int = 100,
                     maxIterations: Int = 100,
                     tolerance: Float = 0.001f,
                     forgetfulness: Float = 0.0f
                   )

}
