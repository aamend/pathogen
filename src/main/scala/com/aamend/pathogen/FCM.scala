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
