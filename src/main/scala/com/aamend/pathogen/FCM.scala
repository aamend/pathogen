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

import com.aamend.pathogen.Rooster._
import com.aamend.pathogen.Sun._
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD

object FCM {

  val defaultConfig: Config = ConfigBuilder.create.build

  /**
    * Build a Fuzzy Cognitive Map from time related events
    *
    * @param events the RDD of time related events
    * @param config the FCM configuration object
    * @return the causal graph
    */
  def build(events: RDD[Event], config: Config = defaultConfig): Graph[Pathogen, Double] = {
    events
      .observe(config)
      .explain(config)
  }

  implicit class FCMProcessor(events: RDD[Event]) {
    def fcm(config: Config = defaultConfig): Graph[Pathogen, Double] = {
      FCM.build(events, config)
    }
  }

}
