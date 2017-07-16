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

/**
  * @param samplingRate    Frequency used to detect time correlation (Hour, Day, Month, etc.)
  * @param inceptionWindow Whether an extra time must be applied prior to the first observation
  * @param simulations     The number of simulations to run (the more the merrier)
  * @param maxIterations   The maximum number of iterations in Pregel
  * @param tolerance       The absolute zero when convergence is reached
  * @param forgetfulness   Whether a decay factor must be applied at each transition
  */
case class Config(
                   samplingRate: Frequency.Value = Frequency.DAY,
                   inceptionWindow: Int = 0,
                   simulations: Int = 100,
                   maxIterations: Int = 100,
                   tolerance: Float = 0.001f,
                   forgetfulness: Float = 0.0f
                 )

trait ConfigGrammar {

  def withSamplingRate(samplingRate: Frequency.Value): ConfigGrammar

  def withInceptionWindow(inceptionWindow: Int): ConfigGrammar

  def withSimulations(simulations: Int): ConfigGrammar

  def withMaxIterations(maxIterations: Int): ConfigGrammar

  def withTolerance(tolerance: Float): ConfigGrammar

  def withForgetfulness(forgetfulness: Float): ConfigGrammar

  def build: Config

}

object ConfigBuilder {
  def create: ConfigGrammar = new ConfigBuilder()
}

case class ConfigBuilder(
                          samplingRate: Frequency.Value = Frequency.DAY,
                          inceptionWindow: Int = 0,
                          simulations: Int = 100,
                          maxIterations: Int = 100,
                          tolerance: Float = 0.001f,
                          forgetfulness: Float = 0.0f
                        ) extends ConfigGrammar {

  val config: Config = Config(
    samplingRate,
    inceptionWindow,
    simulations,
    maxIterations,
    tolerance,
    forgetfulness
  )

  def withSamplingRate(samplingRate: Frequency.Value) = new ConfigBuilder(
    samplingRate,
    config.inceptionWindow,
    config.simulations,
    config.maxIterations,
    config.tolerance,
    config.forgetfulness
  )

  def withInceptionWindow(inceptionWindow: Int) = new ConfigBuilder(
    config.samplingRate,
    inceptionWindow,
    config.simulations,
    config.maxIterations,
    config.tolerance,
    config.forgetfulness
  )

  def withSimulations(simulations: Int) = new ConfigBuilder(
    config.samplingRate,
    config.inceptionWindow,
    simulations,
    config.maxIterations,
    config.tolerance,
    config.forgetfulness
  )

  def withMaxIterations(maxIterations: Int) = new ConfigBuilder(
    config.samplingRate,
    config.inceptionWindow,
    config.simulations,
    maxIterations,
    config.tolerance,
    config.forgetfulness
  )

  def withTolerance(tolerance: Float) = new ConfigBuilder(
    config.samplingRate,
    config.inceptionWindow,
    config.simulations,
    config.maxIterations,
    tolerance,
    config.forgetfulness
  )

  def withForgetfulness(forgetfulness: Float) = new ConfigBuilder(
    config.samplingRate,
    config.inceptionWindow,
    config.simulations,
    config.maxIterations,
    config.tolerance,
    forgetfulness
  )

  def build: Config = config

}
